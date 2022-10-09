{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module HStream.Server.Handler.Query
  ( -- * For grpc-haskell
    executeQueryHandler
  , executePushQueryHandler
  , terminateQueriesHandler
  , getQueryHandler
  , listQueriesHandler
  , deleteQueryHandler
  , restartQueryHandler
    -- * For hs-grpc-server
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async         (async, cancel, wait)
import           Control.Exception                (Handler (..), handle)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Char8            as BS
import qualified Data.HashMap.Strict              as HM
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import           Data.String                      (IsString (fromString))
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Exception

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types
import           HStream.Server.ConnectorTypes    hiding (StreamName, Timestamp)
import qualified HStream.Server.Core.Query        as Core
import           HStream.Server.Exception         (defaultHandlers,
                                                   defaultServerStreamExceptionHandle)
import           HStream.Server.Handler.Common
import qualified HStream.Server.HStore            as HStore
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData          as P
import qualified HStream.Server.Shard             as Shard
import           HStream.Server.Types
import           HStream.SQL.Codegen              hiding (StreamName)
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-- Other sqls, called in 'sqlAction'
executeQueryHandler :: ServerContext
                    -> ServerRequest 'Normal API.CommandQuery API.CommandQueryResponse
                    -> IO (ServerResponse 'Normal API.CommandQueryResponse)
executeQueryHandler sc (ServerNormalRequest _metadata req) =
  queryExceptionHandle $ returnResp =<< Core.executeQuery sc req

executePushQueryHandler ::
  ServerContext ->
  ServerRequest 'ServerStreaming API.CommandPushQuery Struct ->
  IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler
  ctx@ServerContext {..}
  (ServerWriterRequest meta API.CommandPushQuery{..} streamSend) = defaultServerStreamExceptionHandle $ do
    Log.debug $ "Receive Push Query Request: " <> Log.buildText commandPushQueryQueryText
    plan <- streamCodegen commandPushQueryQueryText
    case plan of
      SelectPlan _ inNodesWithStreams outNodeWithStream _ _ -> do
        let sources = snd <$> inNodesWithStreams
            sink    = snd outNodeWithStream
        exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
        case and exists of
          False -> do
            Log.warning $ "At least one of the streams do not exist: "
              <> Log.buildString (show sources)
            throwIO $ HE.StreamNotFound $ "At least one of the streams do not exist: " <> T.pack (show sources)
          True  -> do
            createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
            let query = P.StreamQuery sources sink
            -- run task
            (qid,_) <- handleCreateAsSelect ctx plan commandPushQueryQueryText query
            tid <- readMVar runningQueries >>= \hm -> return $ (HM.!) hm qid

            -- sub from sink stream and push to client
            consumerName <- newRandomText 20
            let sc = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
            subscribeToStreamWithoutCkp sc sink API.SpecialOffsetLATEST

            sending <- async (sendToClient zkHandle qid sink sc streamSend)

            void . forkIO $ handlePushQueryCanceled meta $ do
              killThread tid
              cancel sending
              P.setQueryStatus qid Terminated zkHandle
              unSubscribeToStreamWithoutCkp sc sink

            wait sending

      _ -> do
        Log.fatal "Push Query: Inconsistent Method Called"
        returnServerStreamingResp StatusInternal "inconsistent method called"

createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
createStreamWithShard client streamId shardName factor = do
  S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
  let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
  void $ S.createStreamPartition client streamId (Just shardName) extrAttr

--------------------------------------------------------------------------------

sendToClient ::
  MetaHandle ->
  T.Text ->
  T.Text ->
  SourceConnectorWithoutCkp ->
  (Struct -> IO (Either GRPCIOError ())) ->
  IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid streamName SourceConnectorWithoutCkp {..} streamSend = do
  let f (e :: ZooException) = do
        Log.fatal $ "ZooKeeper Exception: " <> Log.buildString (show e)
        return $ ServerWriterResponse [] StatusAborted "failed to get status"
  handle f $
    do
      P.getQueryStatus qid zkHandle
      >>= \case
        Terminated -> return (ServerWriterResponse [] StatusAborted "")
        Created -> return (ServerWriterResponse [] StatusAlreadyExists "")
        Running -> do
          withReadRecordsWithoutCkp streamName $ \sourceRecords -> do
            let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
                structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
            void $ streamSendMany structs
          return (ServerWriterResponse [] StatusOk "")
        _ -> return (ServerWriterResponse [] StatusUnknown "")
  where
    streamSendMany = \case
      []        -> return (ServerWriterResponse [] StatusOk "")
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            return (ServerWriterResponse [] StatusInternal (fromString (show err)))
          Right _ -> streamSendMany xs'

--------------------------------------------------------------------------------

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ListQueriesRequest API.ListQueriesResponse
  -> IO (ServerResponse 'Normal API.ListQueriesResponse)
listQueriesHandler ctx (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Query Request"
  Core.listQueries ctx >>= returnResp . (API.ListQueriesResponse . V.fromList)

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.GetQueryRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
getQueryHandler ctx (ServerNormalRequest _metadata req@API.GetQueryRequest{..}) = do
  Log.debug $ "Receive Get Query Request. "
    <> "Query ID: " <> Log.buildText getQueryRequestId
  Core.getQuery ctx req >>= \case
    Just q -> returnResp q
    _      -> returnErrResp StatusNotFound "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal API.TerminateQueriesRequest API.TerminateQueriesResponse
  -> IO (ServerResponse 'Normal API.TerminateQueriesResponse)
terminateQueriesHandler ctx (ServerNormalRequest _metadata req@API.TerminateQueriesRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  Core.terminateQueries ctx req >>= \case
    Left terminatedQids -> do
      returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    Right resp -> returnResp resp

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ctx (ServerNormalRequest _metadata req@API.DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildText deleteQueryRequestId
    Core.deleteQuery ctx req
    returnResp Empty

-- FIXME: Incorrect implementation!
restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.RestartQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartQueryHandler _ (ServerNormalRequest _metadata _) = do
  Log.fatal "Restart Query Not Supported"
  returnErrResp StatusUnimplemented "restart query not suppported yet"
    -- queries <- P.withMaybeZHandle zkHandle P.getQueries
    -- case find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == restartQueryRequestId) queries of
    --   Just query -> do
    --     P.withMaybeZHandle zkHandle $ P.setQueryStatus (P.queryId query) P.Running
    --     returnResp Empty
      -- Nothing    -> returnErrResp StatusInternal "Query does not exist"

--------------------------------------------------------------------------------
-- Exception and Exception Handler

sqlExceptionHandlers :: [HE.Handler (StatusCode, StatusDetails)]
sqlExceptionHandlers =[
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails . BS.pack . formatSomeSQLException $ err))
  ]

queryExceptionHandle :: HE.ExceptionHandle (ServerResponse 'Normal a)
queryExceptionHandle = HE.mkExceptionHandle . HE.setRespType mkServerErrResp $
  sqlExceptionHandlers ++ defaultHandlers
