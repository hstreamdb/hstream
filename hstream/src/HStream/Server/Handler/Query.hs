{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import           Data.String                      (IsString (fromString))
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite.JSONPB              as PB
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Exception
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Connector.Common         (SourceConnectorWithoutCkp (..))
import           HStream.Connector.Type           hiding (StreamName, Timestamp)
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Query        as Core
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
    SelectPlan {} -> returnErrResp StatusInvalidArgument "Inconsistent method called"
    CreateConnectorPlan {} -> do
      IO.createIOTaskFromSql scIOWorker commandQueryStmtText >> returnCommandQueryEmptyResp
      -- connector <- IO.createIOTaskFromSql scIOWorker commandQueryStmtText
      -- returnCommandQueryResp (mkVectorStruct connector "created_connector")
    CreateBySelectPlan _ inNodesWithStreams outNodeWithStream _ _ _ -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.StreamQuery (textToCBytes <$> sources) (textToCBytes sink)
      create (transToStreamName sink)
      (qid,_) <- handleCreateAsSelect sc plan commandQueryStmtText query
      returnCommandQueryResp (mkVectorStruct (cBytesToText qid) "stream_query_id")
    CreateViewPlan _ schema inNodesWithStreams outNodeWithStream _ _ accumulation -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema
      -- make sure source streams exist
      existedStreams <- V.toList <$> Core.listStreamNames sc
      existedViews   <-              Core.listViewNames   sc
      let existSources = existedStreams <> existedViews
      case L.find (`notElem` existSources) sources of
        Nothing -> do
          create (transToStreamName sink)
          (qid,_) <- handleCreateAsSelect sc plan commandQueryStmtText query
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink accumulation hm, ()))
          returnCommandQueryResp (mkVectorStruct (cBytesToText qid) "view_query_id")
        Just nonExistedSource -> do
          returnErrResp StatusInvalidArgument . StatusDetails . BS.pack $
            "Source " <> show nonExistedSource <> " doesn't exist"
    CreatePlan stream fac -> do
      let s = API.Stream
            { streamStreamName = stream
            , streamReplicationFactor = fromIntegral fac
            , streamBacklogDuration = 0
            , streamShardCount = 1
            }
      Core.createStream sc s
      returnCommandQueryResp (mkVectorStruct s "created_stream")
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
          void $ Core.deleteView sc view checkIfExist
          returnCommandQueryEmptyResp
        DConnector conn -> do
          IO.deleteIOTask scIOWorker conn
          returnCommandQueryEmptyResp
    ShowPlan showObject ->
      case showObject of
        SStreams -> do
          streams <- Core.listStreams sc ListStreamsRequest
          returnCommandQueryResp (mkVectorStruct streams "streams")
        SQueries -> do
          queries <- Core.listQueries sc
          returnCommandQueryResp (mkVectorStruct (V.fromList queries) "queries")
        SConnectors -> do
          connectors <- IO.listIOTasks scIOWorker
          returnCommandQueryResp (mkVectorStruct (V.fromList connectors) "connectors")
        SViews -> do
          views <- Core.listViews sc
          returnCommandQueryResp (mkVectorStruct (V.fromList views) "views")
    TerminatePlan sel -> do
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
      Core.terminateQueries sc request >>= \case
        Left terminatedQids -> returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
        Right TerminateQueriesResponse{..} -> do
          let value  = PB.toAesonValue terminateQueriesResponseQueryId
              object = HM.fromList [("terminated", value)]
              result = V.singleton (jsonObjectToStruct object)
          returnCommandQueryResp result
    SelectViewPlan RSelectView {..} -> do
      hm <- readIORef P.groupbyStores
      case HM.lookup rSelectViewFrom hm of
        Nothing -> returnErrResp StatusNotFound "VIEW not found"
        Just accumulation -> do
          DataChangeBatch{..} <- readMVar accumulation
          case dcbChanges of
            [] -> sendResp mempty
            _  -> do
              sendResp $ V.map (dcRow . mapView rSelectViewSelect)
                (V.fromList $ filterView rSelectViewWhere dcbChanges)
    ExplainPlan _ -> do
      undefined
      {-
      execPlan <- genExecutionPlan sql
      let object = HM.fromList [("PLAN", Aeson.String . T.pack $ show execPlan)]
      returnCommandQueryResp $ V.singleton (jsonObjectToStruct object)
      -}
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False >> returnCommandQueryEmptyResp
    ResumePlan (ResumeObjectConnector name) -> do
      IO.startIOTask scIOWorker name >> returnCommandQueryEmptyResp
    _ -> discard
  where
    create sName = do
      Log.debug . Log.buildString $ "CREATE: new stream " <> show sName
      createStreamWithShard scLDClient sName "query" scDefaultStreamRepFactor

    -- TODO: return correct RPC name
    discard = (Log.warning . Log.buildText) "impossible happened" >> returnErrResp StatusInternal "discarded method called"

    sendResp results = returnCommandQueryResp $
      V.map (structToStruct "SELECTVIEW" . jsonObjectToStruct) results
    mkVectorStruct a label =
      let object = HM.fromList [(label, PB.toAesonValue a)]
       in V.singleton (jsonObjectToStruct object)

mapView :: SelectViewSelect -> DataChange a -> DataChange a
mapView SVSelectAll change = change
mapView (SVSelectFields fieldsWithAlias) change@DataChange{..} =
  let newRow = HM.fromList $
               L.map (\(field,alias) ->
                         (T.pack alias, HSC.getFieldByName dcRow field)
                     ) fieldsWithAlias
  in change {dcRow = newRow}

filterView :: RWhere -> [DataChange a] -> [DataChange a]
filterView rwhere =
  L.filter (\DataChange{..} -> HSC.genFilterR rwhere dcRow)

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

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ctx (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Query Request"
  Core.listQueries ctx >>= returnResp . (ListQueriesResponse . V.fromList)

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ctx (ServerNormalRequest _metadata req@GetQueryRequest{..}) = do
  Log.debug $ "Receive Get Query Request. "
    <> "Query ID: " <> Log.buildText getQueryRequestId
  Core.getQuery ctx req >>= \case
    Just q -> returnResp q
    _      -> returnErrResp StatusNotFound "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler ctx (ServerNormalRequest _metadata req@TerminateQueriesRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  Core.terminateQueries ctx req >>= \case
    Left terminatedQids -> do
      returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    Right resp -> returnResp resp

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ctx (ServerNormalRequest _metadata req@DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildText deleteQueryRequestId
    Core.deleteQuery ctx req
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
