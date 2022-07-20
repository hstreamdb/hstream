{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Query where

import           Control.Concurrent
import           Control.Concurrent.Async         (async, cancel, wait)
import           Control.Exception                (Exception, Handler (..),
                                                   handle)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Char8            as BS
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef',
                                                   readIORef)
import           Data.List                        (find, (\\))
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import           Data.String                      (IsString (fromString))
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB
import qualified Z.IO.Network                     as ZNet
import           ZooKeeper.Exception
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Connector.Common         (SourceConnectorWithoutCkp (..))
import           HStream.Connector.Type           hiding (StreamName, Timestamp)
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Config
import qualified HStream.Server.Core.Common       as Core
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.View         as Core
import           HStream.Server.Exception
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.Persistence       as P
import qualified HStream.Server.Shard             as Shard
import           HStream.Server.Types
import           HStream.SQL.AST
import           HStream.SQL.Codegen              hiding (StreamName)
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import qualified HStream.SQL.Internal.Codegen     as HSC
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

import           DiffFlow.Types                   (DataChange (..),
                                                   DataChangeBatch (..))

-- Other sqls, called in 'sqlAction'
executeQueryHandler :: ServerContext
                    -> ServerRequest 'Normal CommandQuery CommandQueryResponse
                    -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext {..} (ServerNormalRequest _metadata CommandQuery {..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Query Request: " <> Log.buildText commandQueryStmtText
  plan <- streamCodegen commandQueryStmtText
  case plan of
    SelectPlan {} -> returnErrResp StatusInvalidArgument "inconsistent method called"
    CreateConnectorPlan _cType _cName _ifNotExist _cConfig -> do
      IO.createIOTaskFromSql scIOWorker commandQueryStmtText >> returnCommandQueryEmptyResp
    CreateBySelectPlan _ inNodesWithStreams outNodeWithStream _ _ _ -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.StreamQuery (textToCBytes <$> sources) (textToCBytes sink)
      create (transToStreamName sink)
      void $ handleCreateAsSelect sc plan commandQueryStmtText query
      returnCommandQueryEmptyResp
    CreateViewPlan _ schema inNodesWithStreams outNodeWithStream _ _ accumulation -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema
      create (transToStreamName sink)
      void $ handleCreateAsSelect sc plan commandQueryStmtText query
      atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink accumulation hm, ()))
      returnCommandQueryEmptyResp
    CreatePlan stream fac -> do
      let s = API.Stream
            { streamStreamName = stream
            , streamReplicationFactor = fromIntegral fac
            , streamBacklogDuration = 0
            , streamShardCount = 1
            }
      Core.createStream sc s
      returnCommandQueryEmptyResp
    InsertPlan stream insertType payload -> do
      timestamp <- getProtoTimestamp
      let header = case insertType of
            JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp T.empty
            RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp T.empty
      let record = buildRecord header payload
      let request = AppendRequest
                  { appendRequestStreamName = stream
                  , appendRequestRecords = V.singleton record
                  }
      void $ Core.appendStream sc request Nothing
      returnCommandQueryEmptyResp
    DropPlan checkIfExist dropObject ->
      case dropObject of
        DStream stream -> do
          let request = DeleteStreamRequest
                      { deleteStreamRequestStreamName = stream
                      , deleteStreamRequestIgnoreNonExist = not checkIfExist
                      , deleteStreamRequestForce = False
                      }
          Core.deleteStream sc request
          returnCommandQueryEmptyResp
        DView view -> do
          Core.deleteView sc view checkIfExist
          returnCommandQueryEmptyResp
        DConnector conn -> do
          -- FIXME: use Core.*
          IO.deleteIOTask scIOWorker conn
          returnCommandQueryEmptyResp
    -- FIXME: Return non-empty results
    -- FIXME: use Core.*
    ShowPlan showObject ->
      runWithAddr (ZNet.ipv4 "127.0.0.1" (ZNet.PortNumber $ _serverPort serverOpts)) $ \api -> do
        case showObject of
          SStreams -> do
            hstreamApiListStreams api (mkClientNormalRequest 1000 ListStreamsRequest)
            returnCommandQueryEmptyResp
          SQueries -> do
            hstreamApiListQueries api (mkClientNormalRequest 1000 ListQueriesRequest)
            returnCommandQueryEmptyResp
          SConnectors -> do
            hstreamApiListConnectors api (mkClientNormalRequest 1000 ListConnectorsRequest)
            returnCommandQueryEmptyResp
          SViews -> do
            hstreamApiListViews api (mkClientNormalRequest 1000 ListViewsRequest)
            returnCommandQueryEmptyResp
    -- FIXME: use Core.*
    TerminatePlan sel ->
      runWithAddr (ZNet.ipv4 "127.0.0.1" (ZNet.PortNumber $ _serverPort serverOpts)) $ \api -> do
        let request = case sel of
              AllQueries       -> TerminateQueriesRequest
                                  { terminateQueriesRequestQueryId = V.empty
                                  , terminateQueriesRequestAll = True
                                  }
              OneQuery qid     -> TerminateQueriesRequest
                                  { terminateQueriesRequestQueryId = V.singleton $ cBytesToText qid
                                  , terminateQueriesRequestAll = False
                                  }
              ManyQueries qids -> TerminateQueriesRequest
                                  { terminateQueriesRequestQueryId = V.fromList $ cBytesToText <$> qids
                                  , terminateQueriesRequestAll = False
                                  }
        void $ hstreamApiTerminateQueries api (mkClientNormalRequest 1000 request)
        returnCommandQueryEmptyResp
    SelectViewPlan RSelectView {..} -> do
      hm <- readIORef P.groupbyStores
      case HM.lookup rSelectViewFrom hm of
        Nothing -> returnErrResp StatusNotFound "VIEW not found"
        Just accumulation -> do
          DataChangeBatch{..} <- readMVar accumulation
          results <- case dcbChanges of
            [] -> do
              x <- sendResp mempty
              return [x]
            _  -> do
              forM (filterView rSelectViewWhere dcbChanges) $ \change -> do
                sendResp (dcRow $ mapView rSelectViewSelect change)
          return $ L.last results
    ExplainPlan _ -> do
      undefined
      {-
      execPlan <- genExecutionPlan sql
      let object = HM.fromList [("PLAN", Aeson.String . T.pack $ show execPlan)]
      returnCommandQueryResp $ V.singleton (jsonObjectToStruct object)
    StartPlan (StartObjectConnector name) -> do
      IO.startIOTask scIOWorker name >> returnCommandQueryEmptyResp
    StopPlan (StopObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False >> returnCommandQueryEmptyResp
      -}
    _ -> discard
  where
    create sName = do
      Log.debug . Log.buildString $ "CREATE: new stream " <> show sName
      createStreamWithShard scLDClient sName "query" scDefaultStreamRepFactor
    sendResp result =
      returnCommandQueryResp
        (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)
    discard = (Log.warning . Log.buildText) "impossible happened" >> returnErrResp StatusInternal "discarded method called"

mapView :: SelectViewSelect -> DataChange a -> DataChange a
mapView SVSelectAll change = change
mapView (SVSelectFields fieldsWithAlias) change@DataChange{..} =
  let newRow = HM.fromList $
               L.map (\(field,alias) ->
                         (T.pack alias, HSC.getFieldByName dcRow field)
                     ) fieldsWithAlias
  in change {dcRow = newRow}

filterView :: SelectViewCond -> [DataChange a] -> [DataChange a]
filterView (field, rexpr) changes =
  L.filter (\DataChange{..} ->
              let column       = HSC.getFieldByName dcRow field
                  (_,expected) = HSC.genRExprValue rexpr dcRow
               in column == expected
           ) changes

executePushQueryHandler ::
  ServerContext ->
  ServerRequest 'ServerStreaming CommandPushQuery Struct ->
  IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler
  ctx@ServerContext {..}
  (ServerWriterRequest meta CommandPushQuery {..} streamSend) = defaultServerStreamExceptionHandle $ do
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
            throwIO StreamNotExist
          True  -> do
            createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
            let query = P.StreamQuery (textToCBytes <$> sources) (textToCBytes sink)
            -- run task
            (qid,_) <- handleCreateAsSelect ctx plan commandPushQueryQueryText query
            tid <- readMVar runningQueries >>= \hm -> return $ (HM.!) hm qid

            -- sub from sink stream and push to client
            consumerName <- newRandomText 20
            let sc = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
            subscribeToStreamWithoutCkp sc sink SpecialOffsetLATEST

            sending <- async (sendToClient zkHandle qid sink sc streamSend)

            forkIO $ handlePushQueryCanceled meta $ do
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
  ZHandle ->
  CB.CBytes ->
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

hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _ _) =
  Query
  { queryId          = cBytesToText queryId
  , queryStatus      = getPBStatus status
  , queryCreatedTime = createdTime
  , queryQueryText   = sqlStatement
  }

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Query Request"
  queries <- P.getQueries zkHandle
  let records = map hstreamQueryToQuery queries
  let resp    = ListQueriesResponse . V.fromList $ records
  returnResp resp

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  Log.debug $ "Receive Get Query Request. "
    <> "Query ID: " <> Log.buildText getQueryRequestId
  query <- do
    queries <- P.getQueries zkHandle
    return $ find (\P.PersistentQuery{..} -> cBytesToText queryId == getQueryRequestId) queries
  case query of
    Just q -> returnResp $ hstreamQueryToQuery q
    _      -> returnErrResp StatusNotFound "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueriesRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  qids <-
    if terminateQueriesRequestAll
      then HM.keys <$> readMVar runningQueries
      else return . V.toList $ textToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- Core.handleQueryTerminate sc (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then do
      Log.warning $ "Following queries cannot be terminated: "
        <> Log.buildString (show $ qids \\ terminatedQids)
      returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    else returnResp $ TerminateQueriesResponse (V.fromList $ cBytesToText <$> terminatedQids)

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildText deleteQueryRequestId
    P.removeQuery (textToCBytes deleteQueryRequestId) zkHandle
    returnResp Empty

-- FIXME: Incorrect implementation!
restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartQueryRequest Empty
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

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

queryExceptionHandlers :: Handlers (StatusCode, StatusDetails)
queryExceptionHandlers =[
  Handler (\(err :: QueryTerminatedOrNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, "Query is already terminated or does not exist"))
  ]

sqlExceptionHandlers :: Handlers (StatusCode, StatusDetails)
sqlExceptionHandlers =[
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails . BS.pack . formatSomeSQLException $ err))
  ]

queryExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
queryExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  sqlExceptionHandlers ++ queryExceptionHandlers ++
  connectorExceptionHandlers ++ defaultHandlers
