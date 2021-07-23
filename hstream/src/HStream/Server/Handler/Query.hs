{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Query where

import           Control.Concurrent               (forkIO, putMVar, takeMVar)
import           Control.Exception                (SomeException, catch, try)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log

import qualified HStream.Connector.HStore         as HCH
import           HStream.Processing.Connector     (subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..))
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   runTaskWrapper)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cBytesToText, returnErrResp,
                                                   returnResp, textToCBytes)

hstreamQueryToQuery :: HSP.Query -> Query
hstreamQueryToQuery (HSP.Query queryId (HSP.Info sqlStatement createdTime) _ (HSP.Status status _)) =
  Query (TL.pack $ ZDC.unpack queryId) (fromIntegral $ fromEnum status) createdTime (TL.pack $ ZT.unpack sqlStatement)

hstreamQueryNameIs :: T.Text -> HSP.Query -> Bool
hstreamQueryNameIs name (HSP.Query queryId _ _ _) = cBytesToText queryId == name

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
createQueryHandler ServerContext{..} (ServerNormalRequest _ CreateQueryRequest{..}) = do
  plan' <- try $ HSC.streamCodegen (TL.toStrict createQueryRequestQueryText)
  err <- case plan' of
    Left  (_ :: SomeSQLException) -> return $ Left "exception on parsing or codegen"
    Right (HSC.SelectPlan sources sink taskBuilder) -> do
      let taskBuilder' = taskBuilderWithName taskBuilder $ T.pack (TL.unpack createQueryRequestId)
      exists <- mapM (HS.doesStreamExists scLDClient . HCH.transToStreamName) sources
      if (not . and) exists then return $ Left "some source stream do not exist"
      else do
        e' <- try $ HS.createStream scLDClient (HCH.transToTempStreamName sink)
          (HS.LogAttrs $ HS.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        case e' of
          Left (_ :: SomeException) -> return $ Left "error when creating sink stream."
          Right _                   -> do
            -- create persistent query
            (qid, timestamp) <- HSP.createInsertPersistentQuery (getTaskName taskBuilder')
              createQueryRequestQueryText (HSP.PlainQuery $ textToCBytes <$> sources) zkHandle
            -- run task
            tid <- forkIO $ HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Running)
              >> runTaskWrapper True taskBuilder' scLDClient
            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            ldreader' <- HS.newLDRsmCkpReader scLDClient
              (textToCBytes (T.append (getTaskName taskBuilder') "-result"))
              HS.checkpointStoreLogID 5000 1 Nothing 10
            let sc = HCH.hstoreTempSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            return $ Right $ Query (TL.pack $ ZDC.unpack qid) (fromIntegral $ fromEnum HSP.Running) timestamp createQueryRequestQueryText
    Right _ -> return $ Left "inconsistent method called"
  case err of
    Left err'   -> do
      Log.fatal . string8 $ err'
      returnErrResp StatusInternal "Failed"
    Right query -> returnResp query

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
  let records = map hstreamQueryToQuery queries
  let resp = ListQueriesResponse . V.fromList $ records
  returnResp resp

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  query <- do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    return $ find (hstreamQueryNameIs (T.pack $ TL.unpack getQueryRequestId)) queries
  case query of
    Just q -> returnResp $ hstreamQueryToQuery q
    _      -> returnErrResp StatusInternal "Not exist"

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) = do
  catch
    ((HSP.withMaybeZHandle zkHandle $ HSP.removeQuery (ZDC.pack $ TL.unpack deleteQueryRequestId)) >> returnResp Empty)
    (\(_ :: SomeException) -> returnErrResp StatusInternal "Failed")

restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartQueryHandler ServerContext{..} (ServerNormalRequest _metadata RestartQueryRequest{..}) = do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack $ TL.unpack restartQueryRequestId)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Running)
        returnResp Empty
      Nothing    -> returnErrResp StatusInternal ""

cancelQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CancelQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
cancelQueryHandler ServerContext{..} (ServerNormalRequest _metadata CancelQueryRequest{..}) = do
  queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
  case find (hstreamQueryNameIs (T.pack $ TL.unpack cancelQueryRequestId)) queries of
    Just query -> do
      _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Terminated)
      returnResp Empty
    Nothing    -> returnErrResp StatusInternal ""
