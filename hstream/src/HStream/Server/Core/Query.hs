{-# LANGUAGE CPP               #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Core.Query
  ( executeQuery
  , terminateQueries

  , createQuery
  , listQueries
  , getQuery
  , deleteQuery

  , hstreamQueryToQuery

  , restoreQuery
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async         (async, cancel, wait)
import           Control.Exception                (throw, throwIO)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef',
                                                   readIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel           (StreamSend)
import           Network.GRPC.HighLevel.Generated (GRPCIOError)
import           Network.GRPC.HighLevel.Server    (ServerCallMetadata)
import qualified Proto3.Suite.JSONPB              as PB
import qualified Z.Data.CBytes                    as CB

import qualified HStream.Exception                as HE
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.Core.Common
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.View         as Core
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData
import qualified HStream.Server.MetaData          as P
import qualified HStream.Server.Shard             as Shard
import           HStream.Server.Types
import           HStream.SQL.AST
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import qualified HStream.Utils.Aeson              as AesonComp
import           HStream.Utils.Validation         (validateNameAndThrow)
#ifdef HStreamUseV2Engine
import           DiffFlow.Types                   (DataChange (..),
                                                   DataChangeBatch (..),
                                                   emptyDataChangeBatch)
import           HStream.Server.ConnectorTypes    hiding (StreamName, Timestamp)
import           HStream.SQL.Codegen
import qualified HStream.SQL.Codegen              as HSC
#else
import           Data.IORef
import           HStream.Processing.Connector
import qualified HStream.Processing.Processor     as HP
import           HStream.Processing.Type
import           HStream.SQL.Codegen.V1
import           HStream.SQL.Codegen.V1           as HSC
#endif

#ifdef HStreamUseV2Engine
-------------------------------------------------------------------------------
executeQuery :: ServerContext -> CommandQuery -> IO CommandQueryResponse
executeQuery sc@ServerContext{..} CommandQuery{..} = do
  Log.debug $ "Receive Query Request: " <> Log.build commandQueryStmtText
  plan <- streamCodegen commandQueryStmtText
  case plan of
    PushSelectPlan {} ->
      let x = "Inconsistent method called: select from streams SQL statements should be sent to rpc `ExecutePushQuery`"
       in throwIO $ HE.InvalidSqlStatement x
    SelectPlan ins out builder -> do
      let sources = inStream <$> ins
      roles_m <- mapM (findIdentifierRole sc) sources
      case all (== Just RoleView) roles_m of
        False -> do
          Log.warning "Can not perform non-pushing SELECT on streams."
          throwIO $ HE.InvalidSqlStatement "Can not perform non-pushing SELECT on streams."
        True  -> do
          out_m <- newMVar emptyDataChangeBatch
          runImmTask sc (ins `zip` L.map fromJust roles_m) out out_m builder
          dcb@DataChangeBatch{..} <- readMVar out_m
          case dcbChanges of
            [] -> sendResp mempty
            _  -> do
              sendResp $ V.map (flowObjectToJsonObject . dcRow) (V.fromList dcbChanges)
    CreateBySelectPlan stream ins out builder factor -> do
      validateNameAndThrow stream
      let sources = inStream <$> ins
          sink    = stream
      roles_m <- mapM (findIdentifierRole sc) sources
      case all isJust roles_m of
        False -> do
          Log.warning $ "At least one of the streams/views do not exist: "
              <> Log.buildString (show sources)
          -- FIXME: use another exception or find which resource doesn't exist
          throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
        True  -> do
          createStreamWithShard scLDClient (transToStreamName sink) "query" factor
          let relatedStreams = (sources, sink)
          P.QueryInfo{..} <- handleCreateAsSelect
                     sc
                     sink
                     (ins `zip` L.map fromJust roles_m)
                     (out, RoleStream)
                     builder
                     commandQueryStmtText
                     relatedStreams
          pure $ API.CommandQueryResponse (mkVectorStruct queryId "stream_query_id")
    CreateViewPlan view ins out builder accumulation -> do
      validateNameAndThrow view
      P.ViewInfo{viewQuery=P.QueryInfo{..}} <-
        Core.createView' sc view ins out builder accumulation commandQueryStmtText
      pure $ API.CommandQueryResponse (mkVectorStruct queryId "view_query_id")
    CreatePlan stream fac -> do
      validateNameAndThrow stream
      let s = API.Stream
            { streamStreamName = stream
            , streamReplicationFactor = fromIntegral fac
            , streamBacklogDuration = 0
            , streamShardCount = 1
            }
      Core.createStream sc s
      pure $ API.CommandQueryResponse (mkVectorStruct s "created_stream")
    CreateConnectorPlan _ cName _ _ _ -> do
      validateNameAndThrow cName
      void $ createIOTaskFromSql sc commandQueryStmtText
      pure $ CommandQueryResponse V.empty
    InsertPlan {} -> discard "Append"
    DropPlan checkIfExist dropObject ->
      case dropObject of
        DStream stream -> do
          let request = DeleteStreamRequest
                      { deleteStreamRequestStreamName = stream
                      , deleteStreamRequestIgnoreNonExist = not checkIfExist
                      , deleteStreamRequestForce = False
                      }
          Core.deleteStream sc request
          pure $ API.CommandQueryResponse V.empty
        DView view -> do
          void $ Core.deleteView sc view checkIfExist
          pure $ API.CommandQueryResponse V.empty
        DConnector conn -> do
          IO.deleteIOTask scIOWorker conn
          pure $ API.CommandQueryResponse V.empty
    ShowPlan showObject ->
      case showObject of
        SStreams -> do
          streams <- Core.listStreams sc ListStreamsRequest
          pure $ API.CommandQueryResponse (mkVectorStruct streams "streams")
        SQueries -> do
          queries <- listQueries sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList queries) "queries")
        SConnectors -> do
          connectors <- IO.listIOTasks scIOWorker
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList connectors) "connectors")
        SViews -> do
          views <- Core.listViews sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList views) "views")
    TerminatePlan sel -> do
      let request = case sel of
            AllQueries       -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.empty
                                , terminateQueriesRequestAll = True
                                }
            OneQuery qid     -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.singleton qid
                                , terminateQueriesRequestAll = False
                                }
            ManyQueries qids -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.fromList qids
                                , terminateQueriesRequestAll = False
                                }
      TerminateQueriesResponse{..} <- terminateQueries sc request
      let value  = PB.toAesonValue terminateQueriesResponseQueryId
          object = AesonComp.fromList [("terminated", value)]
          result = V.singleton (jsonObjectToStruct object)
      pure $ API.CommandQueryResponse result
    ExplainPlan plan -> pure $ API.CommandQueryResponse (mkVectorStruct plan "explain")
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False
      pure (CommandQueryResponse V.empty)
    ResumePlan (ResumeObjectConnector name) -> do
      IO.startIOTask scIOWorker name
      pure (CommandQueryResponse V.empty)
  where
    discard rpcName = throwIO $ HE.DiscardedMethod $
      "Discarded method called: should call rpc `" <> rpcName <> "` instead"
    sendResp results = pure $ API.CommandQueryResponse $
      V.map (structToStruct "SELECTVIEW" . jsonObjectToStruct) results
    mkVectorStruct a label =
      let object = AesonComp.fromList [(label, PB.toAesonValue a)]
       in V.singleton (jsonObjectToStruct object)
#else
-------------------------------------------------------------------------------
executeQuery :: ServerContext -> CommandQuery -> IO CommandQueryResponse
executeQuery sc@ServerContext{..} CommandQuery{..} = do
  Log.debug $ "Receive Query Request: " <> Log.build commandQueryStmtText
  plan <- streamCodegen commandQueryStmtText
  case plan of
    PushSelectPlan {} ->
      let x = "Inconsistent method called: select from streams SQL statements should be sent to rpc `ExecutePushQuery`"
       in throwIO $ HE.InvalidSqlStatement x
    SelectPlan sources sink builder persist -> do
      roles_m <- mapM (findIdentifierRole sc) sources
      case all (== Just RoleView) roles_m of
        False -> do
          Log.warning "Can not perform non-pushing SELECT on streams."
          throwIO $ HE.InvalidSqlStatement "Can not perform non-pushing SELECT on streams."
        True  -> do
          hm <- readIORef P.groupbyStores
          let mats = L.map ((HM.!) hm) sources

          sinkRecords_m <- newIORef []
          let sinkConnector = HStore.memorySinkConnector sinkRecords_m
          HP.runImmTask (sources `zip` mats) sinkConnector builder () Just Just
          sinkRecords <- readIORef sinkRecords_m

          let flowObjects = (L.map (fromJust . Aeson.decode . snkValue) sinkRecords) :: [FlowObject]
          case flowObjects of
            [] -> sendResp mempty
            _  -> sendResp (V.fromList $ flowObjectToJsonObject <$> flowObjects)
    CreateBySelectPlan sources sink builder factor persist -> do
      validateNameAndThrow sink
      roles_m <- mapM (findIdentifierRole sc) sources
      case all isJust roles_m of
        False -> do
          Log.warning $ "At least one of the streams/views do not exist: "
              <> Log.buildString (show sources)
          throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
        True  ->
          -- TODO: Support joining between streams and views
          case all (== Just RoleStream) roles_m of
            False -> do
              Log.warning "CREATE STREAM only supports sources of stream type"
              throwIO $ HE.InvalidSqlStatement "CREATE STREAM only supports sources of stream type"
            True  -> do
              createStreamWithShard scLDClient (transToStreamName sink) "query" factor
              let relatedStreams = (sources, sink)
              queryId <- newRandomText 10
              P.QueryInfo{..} <- handleCreateAsSelect sc builder queryId commandQueryStmtText relatedStreams True
              pure $ API.CommandQueryResponse (mkVectorStruct queryId "stream_query_id")
    CreateViewPlan sources sink view builder persist -> do
      validateNameAndThrow sink
      validateNameAndThrow view
      P.ViewInfo{viewQuery=P.QueryInfo{..}} <-
        Core.createView' sc view sources sink builder persist commandQueryStmtText
      pure $ API.CommandQueryResponse (mkVectorStruct queryId "view_query_id")
    CreatePlan stream fac -> do
      validateNameAndThrow stream
      let s = API.Stream
            { streamStreamName = stream
            , streamReplicationFactor = fromIntegral fac
            , streamBacklogDuration = 0
            , streamShardCount = 1
            }
      Core.createStream sc s
      pure $ API.CommandQueryResponse (mkVectorStruct s "created_stream")
    CreateConnectorPlan _ cName _ _ _ -> do
      validateNameAndThrow cName
      void $ createIOTaskFromSql sc commandQueryStmtText
      pure $ CommandQueryResponse V.empty
    InsertPlan {} -> discard "Append"
    DropPlan checkIfExist dropObject ->
      case dropObject of
        DStream stream -> do
          let request = DeleteStreamRequest
                      { deleteStreamRequestStreamName = stream
                      , deleteStreamRequestIgnoreNonExist = not checkIfExist
                      , deleteStreamRequestForce = False
                      }
          Core.deleteStream sc request
          pure $ API.CommandQueryResponse V.empty
        DView view -> do
          void $ Core.deleteView sc view checkIfExist
          pure $ API.CommandQueryResponse V.empty
        DConnector conn -> do
          IO.deleteIOTask scIOWorker conn
          pure $ API.CommandQueryResponse V.empty
    ShowPlan showObject ->
      case showObject of
        SStreams -> do
          streams <- Core.listStreams sc ListStreamsRequest
          pure $ API.CommandQueryResponse (mkVectorStruct streams "streams")
        SQueries -> do
          queries <- listQueries sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList queries) "queries")
        SConnectors -> do
          connectors <- IO.listIOTasks scIOWorker
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList connectors) "connectors")
        SViews -> do
          views <- Core.listViews sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList views) "views")
    TerminatePlan sel -> do
      let request = case sel of
            AllQueries       -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.empty
                                , terminateQueriesRequestAll = True
                                }
            OneQuery qid     -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.singleton qid
                                , terminateQueriesRequestAll = False
                                }
            ManyQueries qids -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.fromList qids
                                , terminateQueriesRequestAll = False
                                }
      TerminateQueriesResponse{..} <- terminateQueries sc request
      let value  = PB.toAesonValue terminateQueriesResponseQueryId
          object = AesonComp.fromList [("terminated", value)]
          result = V.singleton (jsonObjectToStruct object)
      pure $ API.CommandQueryResponse result
    ExplainPlan plan -> pure $ API.CommandQueryResponse (mkVectorStruct plan "explain")
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False
      pure (CommandQueryResponse V.empty)
    ResumePlan (ResumeObjectConnector name) -> do
      IO.startIOTask scIOWorker name
      pure (CommandQueryResponse V.empty)
  where
    discard rpcName = throwIO $ HE.DiscardedMethod $
      "Discarded method called: should call rpc `" <> rpcName <> "` instead"
    sendResp results = pure $ API.CommandQueryResponse $
      V.map (structToStruct "SELECTVIEW" . jsonObjectToStruct) results
    mkVectorStruct a label =
      let object = AesonComp.fromList [(label, PB.toAesonValue a)]
       in V.singleton (jsonObjectToStruct object)
#endif

sendToClient
  :: MetaHandle
  -> T.Text
  -> T.Text
  -> SourceConnectorWithoutCkp
  -> (Struct -> IO (Either GRPCIOError ()))
  -> IO ()
sendToClient metaHandle qid streamName SourceConnectorWithoutCkp{..} streamSend = do
  M.getMeta @P.QueryStatus qid metaHandle >>= \case
    Just Terminated -> throwIO $ HE.PushQueryTerminated ""
    Just Created -> throwIO $ HE.PushQueryCreated ""
    Just Running -> do
      withReadRecordsWithoutCkp streamName Just Just $ \sourceRecords -> do
        let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
            structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
        return (void $ streamSendMany structs, return ())
    _ -> throwIO $ HE.UnknownPushQueryStatus ""
    . (P.queryState <$>)
  where
    streamSendMany = \case
      []        -> pure ()
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            throwIO $ HE.PushQuerySendError (show err)
          Right _ -> streamSendMany xs'

createQuery ::
  ServerContext -> CreateQueryRequest -> IO Query
createQuery
  sc@ServerContext {..} CreateQueryRequest {..} = do
    plan <- streamCodegen createQueryRequestSql
    case plan of
      CreateBySelectPlan{} -> do
        CommandQueryResponse{..} <-
          executeQuery sc CommandQuery{commandQueryStmtText = createQueryRequestSql}
        let (PB.Struct kvmap) = V.head commandQueryResponseResultSet
            [(_,qid_m)] = Map.toList kvmap
            (Just (PB.Value (Just (PB.ValueKindStringValue qid)))) = qid_m
        getMeta @P.QueryInfo qid metaHandle >>= \case
          Just pQuery -> hstreamQueryToQuery metaHandle pQuery
          Nothing     -> throwIO $ HE.UnexpectedError "Failed to create query for some unknown reason"
      _ -> throw $ HE.WrongExecutionPlan "Create query only support select / create stream as select statements"

listQueries :: ServerContext -> IO [Query]
listQueries ServerContext{..} = do
  queries <- M.listMeta metaHandle
  mapM (hstreamQueryToQuery metaHandle) queries

getQuery :: ServerContext -> GetQueryRequest -> IO Query
getQuery ctx req@GetQueryRequest{..} = do
  m_query <- getQuery' ctx req
  maybe (throwIO $ HE.QueryNotFound getQueryRequestId) pure m_query

getQuery' :: ServerContext -> GetQueryRequest -> IO (Maybe Query)
getQuery' ServerContext{..} GetQueryRequest{..} = do
  queries <- M.listMeta metaHandle
  hstreamQueryToQuery metaHandle `traverse`
    L.find (\P.QueryInfo{..} -> queryId == getQueryRequestId) queries

terminateQueries
  :: ServerContext -> TerminateQueriesRequest -> IO TerminateQueriesResponse
terminateQueries ctx req = terminateQueries' ctx req >>= \case
  Left terminatedQids ->
    let x = "Only the following queries are terminated " <> show terminatedQids
     in throwIO $ HE.TerminateQueriesError x
  Right r -> pure r

terminateQueries'
  :: ServerContext
  -> TerminateQueriesRequest
  -> IO (Either [T.Text] TerminateQueriesResponse)
terminateQueries' ctx@ServerContext{..} TerminateQueriesRequest{..} = do
  qids <- if terminateQueriesRequestAll
          then HM.keys <$> readMVar runningQueries
          else return . V.toList $ terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate ctx (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then return (Left terminatedQids)
    else return (Right $ TerminateQueriesResponse (V.fromList terminatedQids))

deleteQuery :: ServerContext -> DeleteQueryRequest -> IO ()
deleteQuery ServerContext{..} DeleteQueryRequest{..} =
  M.deleteMeta @P.QueryInfo deleteQueryRequestId Nothing metaHandle

----
-- Warning: may throw exceptions if restoration fails
restoreQuery :: ServerContext -> T.Text -> IO ()
restoreQuery ctx@ServerContext{..} queryId = do
  getMeta @P.QueryInfo queryId metaHandle >>= \case
    Just qInfo@P.QueryInfo{..} -> do
      plan <- streamCodegen querySql
      case plan of
        CreateBySelectPlan sources sink builder _ _ ->
          void $ handleCreateAsSelect ctx builder queryId querySql queryStreams True
        CreateViewPlan sources sink view builder _ ->
          void $ handleCreateAsSelect ctx builder queryId querySql queryStreams False
        _ -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> "should not be like \"" <> T.unpack querySql <> "\". What happened?")
    Nothing -> throwIO $ HE.QueryNotFound ("Query " <> queryId <> "does not exist")

-------------------------------------------------------------------------------

hstreamQueryToQuery :: MetaHandle -> P.QueryInfo -> IO Query
hstreamQueryToQuery h P.QueryInfo{..} = do
  state <- getMeta @P.QueryStatus queryId h >>= \case
    Nothing                -> return Unknown
    Just P.QueryStatus{..} -> return queryState
  return Query
    { queryId = queryId
    , queryQueryText = querySql
    , queryStatus = getPBStatus state
    , queryCreatedTime = queryCreatedTime
    }

-------------------------------------------------------------------------------

createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
createStreamWithShard client streamId shardName factor = do
  S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
  let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
  void $ S.createStreamPartition client streamId (Just shardName) extrAttr
