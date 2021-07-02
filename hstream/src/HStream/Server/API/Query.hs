{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.API.Query where

import           Control.Concurrent               (forkIO, killThread, putMVar,
                                                   takeMVar)
import           Control.Exception                (SomeException, catch, try)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')

import qualified HStream.Connector.HStore         as HCH
import           HStream.Processing.Connector     (subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..))
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.Common            (ServerContext (..),
                                                   checkpointRootPath,
                                                   createInsertPersistentQuery,
                                                   handlePushQueryCanceled,
                                                   runTaskWrapper)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText, textToCBytes)

hstreamQueryToGetQueryResponse :: HSP.Query -> GetQueryResponse
hstreamQueryToGetQueryResponse (HSP.Query queryId (HSP.Info sqlStatement createdTime) _ (HSP.Status status _)) =
  GetQueryResponse (TL.pack $ ZDC.unpack queryId) (fromIntegral $ fromEnum status) createdTime (TL.pack $ ZT.unpack sqlStatement) (Enumerated $ Right HStreamServerErrorNoError)

emptyGetQueryResponse :: GetQueryResponse
emptyGetQueryResponse = GetQueryResponse "" 0 0 "" (Enumerated $ Right HStreamServerErrorNotExistError)

hstreamQueryNameIs :: T.Text -> HSP.Query -> Bool
hstreamQueryNameIs name (HSP.Query queryId _ _ _) = (cbytesToText queryId) == name

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateQueryRequest CreateQueryResponse
  -> IO (ServerResponse 'Normal CreateQueryResponse)
createQueryHandler sc@ServerContext{..} (ServerNormalRequest _ CreateQueryRequest{..}) = do
  plan' <- try $ HSC.streamCodegen $ (TL.toStrict createQueryRequestQueryText)
  err <- case plan' of
    Left  (_ :: SomeSQLException) -> return $ Just "exception on parsing or codegen"
    Right (HSC.SelectPlan sources sink taskBuilder) -> do
      let taskBuilder' = taskBuilderWithName taskBuilder $ T.pack (TL.unpack createQueryRequestId)
      exists <- mapM (HS.doesStreamExists scLDClient . HCH.transToStreamName) sources
      if (not . and) exists then return $ Just "some source stream do not exist"
      else do
        e' <- try $ HS.createTempStream scLDClient (HCH.transToTempStreamName sink)
          (HS.LogAttrs $ HS.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        case e' of
          Left (_ :: SomeException) -> return $ Just "error when creating sink stream."
          Right _                   -> do
            -- create persistent query
            qid <- createInsertPersistentQuery (getTaskName taskBuilder')
              createQueryRequestQueryText HSP.PlainQuery zkHandle
            -- run task
            tid <- forkIO $ HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Running)
              >> runTaskWrapper True taskBuilder' scLDClient
            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            -- _ <- forkIO $ handlePushQueryCanceled _metadata
            --   (killThread tid >> HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Terminated))
            ldreader' <- HS.newLDRsmCkpReader scLDClient
              (textToCBytes (T.append (getTaskName taskBuilder') "-result"))
              HS.checkpointStoreLogID 5000 1 Nothing 10
            let sc = HCH.hstoreTempSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            return Nothing
    Right _ -> return $ Just "inconsistent method called"
  case err of
    Just err -> do
      Log.fatal . string8 $ err
      return (ServerNormalResponse (CreateQueryResponse False) [] StatusOk  "")
    Nothing  -> return (ServerNormalResponse (CreateQueryResponse True) [] StatusOk  "")

fetchQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal FetchQueryRequest FetchQueryResponse
  -> IO (ServerResponse 'Normal FetchQueryResponse)
fetchQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata _) = do
  queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
  let records = map hstreamQueryToGetQueryResponse queries
  let resp = FetchQueryResponse . V.fromList $ records
  return (ServerNormalResponse resp [] StatusOk "")

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest GetQueryResponse
  -> IO (ServerResponse 'Normal GetQueryResponse)
getQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  query <- do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    return $ find (hstreamQueryNameIs (T.pack $ TL.unpack getQueryRequestId)) queries
  let resp = case query of
        Just q -> hstreamQueryToGetQueryResponse q
        _      ->  emptyGetQueryResponse
  return (ServerNormalResponse resp [] StatusOk "")

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest DeleteQueryResponse
  -> IO (ServerResponse 'Normal DeleteQueryResponse)
deleteQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) = do
  res <- catch
    ((HSP.withMaybeZHandle zkHandle $ HSP.removeQuery (ZDC.pack $ TL.unpack deleteQueryRequestId)) >> return True)
    (\(e :: SomeException) -> return False)
  return (ServerNormalResponse (DeleteQueryResponse res) [] StatusOk "")

restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartQueryRequest RestartQueryResponse
  -> IO (ServerResponse 'Normal RestartQueryResponse)
restartQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata RestartQueryRequest{..}) = do
  res <- do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack $ TL.unpack restartQueryRequestId)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Running)
        return True
      Nothing -> return False
  return (ServerNormalResponse (RestartQueryResponse res) [] StatusOk "")

cancelQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CancelQueryRequest CancelQueryResponse
  -> IO (ServerResponse 'Normal CancelQueryResponse)
cancelQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata CancelQueryRequest{..}) = do
  res <- do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack $ TL.unpack cancelQueryRequestId)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Terminated)
        return True
      Nothing -> return False
  return (ServerNormalResponse (CancelQueryResponse res) [] StatusOk "")
