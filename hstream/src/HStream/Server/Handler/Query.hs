{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Query where

import           Control.Concurrent               (readMVar)
import           Control.Exception                (throwIO)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find, (\\))
import qualified Data.Map.Strict                  as Map
import           Data.String                      (IsString (fromString))
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.Text                      as ZT

import qualified HStream.Connector.HStore         as HCH
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector     (subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..))
import qualified HStream.SQL.Codegen              as HSC
import           HStream.Server.Exception         (StreamNotExist (..),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleCreateAsSelect,
                                                   handleQueryTerminate)
import qualified HStream.Server.Persistence       as P
import qualified HStream.Store                    as HS
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cBytesToLazyText,
                                                   lazyTextToCBytes,
                                                   returnErrResp, returnResp,
                                                   textToCBytes)

hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _) =
  Query
  { queryId = cBytesToLazyText queryId
  , queryStatus = fromIntegral $ fromEnum status
  , queryCreatedTime = createdTime
  , queryQueryText = TL.pack $ ZT.unpack sqlStatement
  }

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
createQueryHandler ctx@ServerContext{..} (ServerNormalRequest _ CreateQueryRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Query Request."
    <> "Query ID: " <> Log.buildString (TL.unpack createQueryRequestId)
    <> "Query Command: " <> Log.buildString (TL.unpack createQueryRequestQueryText)
  plan <- HSC.streamCodegen (TL.toStrict createQueryRequestQueryText)
  case plan of
    HSC.SelectPlan sources sink taskBuilder -> do
      let taskBuilder' = taskBuilderWithName taskBuilder $ T.pack (TL.unpack createQueryRequestId)
      exists <- mapM (HS.doesStreamExists scLDClient . HCH.transToStreamName) sources
      if (not . and) exists
      then do
        Log.fatal $ "At least one of the streams do not exist: "
          <> Log.buildString (show sources)
        throwIO StreamNotExist
      else do
        HS.createStream scLDClient (HCH.transToTempStreamName sink)
          (HS.LogAttrs $ HS.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        (qid, timestamp) <- handleCreateAsSelect ctx taskBuilder'
          createQueryRequestQueryText (P.PlainQuery $ textToCBytes <$> sources) HS.StreamTypeTemp
        ldreader' <- HS.newLDRsmCkpReader scLDClient
          (textToCBytes (T.append (getTaskName taskBuilder') "-result"))
          HS.checkpointStoreLogID 5000 1 Nothing 10
        let sc = HCH.hstoreSourceConnector scLDClient ldreader' HS.StreamTypeTemp -- FIXME: view or temp?
        subscribeToStream sc sink Latest
        returnResp $
          Query
          { queryId = cBytesToLazyText qid
          , queryStatus = fromIntegral $ fromEnum P.Running
          , queryCreatedTime = timestamp
          , queryQueryText = createQueryRequestQueryText
          }
    _ -> do
      Log.fatal "Push Query: Inconsistent Method Called"
      returnErrResp StatusInternal "inconsistent method called"

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Query Request"
  queries <- P.withMaybeZHandle zkHandle P.getQueries
  let records = map hstreamQueryToQuery queries
  let resp = ListQueriesResponse . V.fromList $ records
  returnResp resp

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  Log.debug $ "Receive Get Query Request. "
    <> "Query ID: " <> Log.buildString (TL.unpack getQueryRequestId)
  query <- do
    queries <- P.withMaybeZHandle zkHandle P.getQueries
    return $ find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == getQueryRequestId) queries
  case query of
    Just q -> returnResp $ hstreamQueryToQuery q
    _      -> returnErrResp StatusInternal "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueriesRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  qids <-
    if terminateQueriesRequestAll
      then HM.keys <$> readMVar runningQueries
      else return . V.toList $ lazyTextToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate sc (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then do
      Log.warning $ "Following queries cannot be terminated: "
        <> Log.buildString (show $ qids \\ terminatedQids)
      returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    else returnResp $ TerminateQueriesResponse (V.fromList $ cBytesToLazyText <$> terminatedQids)

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) =
  defaultExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildString (TL.unpack deleteQueryRequestId)
    P.withMaybeZHandle zkHandle $ P.removeQuery (lazyTextToCBytes deleteQueryRequestId)
    returnResp Empty

-- FIXME: Incorrect implementation!
restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartQueryHandler ServerContext{..} (ServerNormalRequest _metadata RestartQueryRequest{..}) = do
  Log.fatal "Restart Query Not Supported"
  returnErrResp StatusInternal "restart query not suppported yet"
    -- queries <- P.withMaybeZHandle zkHandle P.getQueries
    -- case find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == restartQueryRequestId) queries of
    --   Just query -> do
    --     P.withMaybeZHandle zkHandle $ P.setQueryStatus (P.queryId query) P.Running
    --     returnResp Empty
      -- Nothing    -> returnErrResp StatusInternal "Query does not exist"
