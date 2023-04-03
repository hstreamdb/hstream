{-# LANGUAGE CPP               #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Core.Query
  ( executeQuery
  , terminateQueries

  , createQuery
  , createQueryWithNamespace
  , listQueries
  , getQuery
  , deleteQuery

  , hstreamQueryToQuery

  , resumeQuery
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
import           Proto3.Suite.Class               (def)
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
import           HStream.SQL
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

-------------------------------------------------------------------------------
executeQuery :: ServerContext -> CommandQuery -> IO CommandQueryResponse
executeQuery sc@ServerContext{..} CommandQuery{..} = do
  Log.debug $ "Receive Query Request: " <> Log.build commandQueryStmtText
  plan <- streamCodegen commandQueryStmtText
  case plan of
#ifdef HStreamUseV2Engine
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
    CreateViewPlan view ins out builder accumulation -> do
      validateNameAndThrow view
      P.ViewInfo{viewQuery=P.QueryInfo{..}} <-
        Core.createView' sc view ins out builder accumulation commandQueryStmtText
      pure $ API.CommandQueryResponse (mkVectorStruct queryId "view_query_id")
#else
    SelectPlan {} -> discard "ExecuteViewQuery"
    CreateViewPlan sources sink view builder persist -> do
      validateNameAndThrow sink
      validateNameAndThrow view
      P.ViewInfo{viewQuery=P.QueryInfo{..}} <-
        Core.createView' sc view sources sink builder persist commandQueryStmtText
      pure $ API.CommandQueryResponse (mkVectorStruct queryId "view_query_id")
#endif
    ExplainPlan plan -> pure $ API.CommandQueryResponse (mkVectorStruct plan "explain")
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False
      pure (CommandQueryResponse V.empty)
    ResumePlan (ResumeObjectConnector name) -> do
      IO.startIOTask scIOWorker name
      pure (CommandQueryResponse V.empty)
    PushSelectPlan {} ->
      let x = "Inconsistent method called: select from streams SQL statements should be sent to rpc `ExecutePushQuery`"
       in throwIO $ HE.InvalidSqlStatement x
    -- NOTE: We may need a handler to be able to execute all available sql statements in the future,
    -- but for the simplicity and readability of this module of code,
    -- we use the handler for sql statements that haven't had its own handler only.
    CreateBySelectPlan {} -> discard "CreateQuery"
    CreatePlan {} -> discard "CreateStream"
    CreateConnectorPlan {} -> discard "CreateConnector"
    InsertPlan {} -> discard "Append"
    DropPlan {} -> discard "Delete"
    ShowPlan {} -> discard "List"
    TerminatePlan {} -> discard "TerminateQuery"
  where
    discard rpcName = throwIO $ HE.DiscardedMethod $
      "Discarded method called: should call rpc `" <> rpcName <> "` instead"
    sendResp results = pure $ API.CommandQueryResponse $
      V.map (structToStruct "SELECTVIEW" . jsonObjectToStruct) results
    mkVectorStruct a label =
      let object = AesonComp.fromList [(label, PB.toAesonValue a)]
       in V.singleton (jsonObjectToStruct object)

sendToClient
  :: MetaHandle
  -> T.Text
  -> T.Text
  -> SourceConnectorWithoutCkp
  -> (Struct -> IO (Either GRPCIOError ()))
  -> IO ()
sendToClient metaHandle qid streamName SourceConnectorWithoutCkp{..} streamSend = do
  M.getMeta @P.QueryStatus qid metaHandle >>= \case
    Just Running -> withReadRecordsWithoutCkp streamName Just Just $ \sourceRecords -> do
      let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
          structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
      return (void $ streamSendMany structs, return ())
    Just _   -> throwIO $ HE.QueryIsNotRunning ""
    _        -> throwIO $ HE.UnknownPushQueryStatus ""

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

-- NOTE: createQueryWithNameSpace may be modified in future.
createQuery :: ServerContext -> CreateQueryRequest -> IO Query
createQuery sc req = createQueryWithNamespace' sc req ""

createQueryWithNamespace ::
  ServerContext -> CreateQueryWithNamespaceRequest -> IO Query
createQueryWithNamespace
  sc@ServerContext {..} CreateQueryWithNamespaceRequest {..} =
  createQueryWithNamespace' sc CreateQueryRequest{
      createQueryRequestSql = createQueryWithNamespaceRequestSql
    , createQueryRequestQueryName = createQueryWithNamespaceRequestQueryName } createQueryWithNamespaceRequestNamespace

createQueryWithNamespace' ::
  ServerContext -> CreateQueryRequest -> T.Text -> IO Query
createQueryWithNamespace'
  sc@ServerContext {..} CreateQueryRequest {..} namespace = do
#ifdef HStreamUseV2Engine
    plan <- streamCodegen createQueryRequestSql
    case plan of
      CreateBySelectPlan stream ins out builder factor -> do
        validateNameAndThrow stream
        let sources = addNamespace . inStream <$> ins  -- namespace
            sink    = addNamespace stream              -- namespace
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
            -- FIXME: pass custom query name
            createQueryAndRun sc sink (ins `zip` L.map fromJust roles_m) (out, RoleStream) builder createQueryRequestSql relatedStreams
            >>= hstreamQueryToQuery metaHandle
      _ -> throw $ HE.WrongExecutionPlan "Create query only support select / create stream as select statements"
#else
    ast <- parseAndRefine createQueryRequestSql
    case ast of
      RQCreate (RCreateAs stream select rOptions) -> hstreamCodegen (RQCreate (RCreateAs (addNamespace stream) (modifySelect select) rOptions)) >>= \case
        CreateBySelectPlan sources sink builder factor persist -> do
          validateNameAndThrow sink
          roles_m <- mapM (findIdentifierRole sc) sources
          unless (all isJust roles_m) $ do
            Log.warning $ "At least one of the streams/views do not exist: "
                <> Log.buildString (show sources)
            throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
          unless (all (== Just RoleStream) roles_m) $ do
            Log.warning "CREATE STREAM only supports sources of stream type"
            throwIO $ HE.InvalidSqlStatement "CREATE STREAM only supports sources of stream type"
          Core.createStream sc def {
              streamStreamName = sink
            , streamReplicationFactor = 1
            , streamBacklogDuration = 7 * 24 * 3600
            , streamShardCount = 1
            }
          let relatedStreams = (sources, sink)
          createQueryAndRun sc QueryRunner {
              qRTaskBuilder = builder
            , qRQueryName   = createQueryRequestQueryName
            , qRQueryString = createQueryRequestSql
            , qRWhetherToHStore = True }
            relatedStreams
          >>= hstreamQueryToQuery metaHandle
        _ -> throw $ HE.WrongExecutionPlan "Create query only support select / create stream as select statements"
      _ -> throw $ HE.WrongExecutionPlan "Create query only support select / create stream as select statements"
  where
    addNamespace = (namespace <>)
    modifySelect :: RSelect -> RSelect
    modifySelect (RSelect a (RFrom t) b c d) = RSelect a (RFrom (modifyTableRef t)) b c d
    modifyTableRef :: RTableRef -> RTableRef
    modifyTableRef (RTableRefSimple                   x my) = RTableRefSimple      (addNamespace x) (addNamespace <$> my)
    -- modifyTableRef (RTableRefSubquery            select mx) = RTableRefSubquery    (modifySelect select)
    modifyTableRef (RTableRefCrossJoin          t1 t2 i) = RTableRefCrossJoin   (modifyTableRef t1) (modifyTableRef t2) i
    modifyTableRef (RTableRefNaturalJoin      t1 j t2 i) = RTableRefNaturalJoin (modifyTableRef t1) j (modifyTableRef t2) i
    modifyTableRef (RTableRefJoinOn         t1 j t2 v i) = RTableRefJoinOn      (modifyTableRef t1) j (modifyTableRef t2) v i
    modifyTableRef (RTableRefJoinUsing   t1 j t2 cols i) = RTableRefJoinUsing   (modifyTableRef t1) j (modifyTableRef t2) cols i

#endif

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
#ifndef HStreamUseV2Engine
resumeQuery :: ServerContext -> T.Text -> IO ()
resumeQuery ctx@ServerContext{..} qRQueryName = do
  getMeta @P.QueryInfo qRQueryName metaHandle >>= \case
    Just qInfo@P.QueryInfo{..} -> do
      (qRTaskBuilder, qRWhetherToHStore) <- streamCodegen querySql >>= \case
        CreateBySelectPlan sources sink builder _ _ -> checkSources sources >> return (builder, True)
        CreateViewPlan sources sink view builder _  -> checkSources sources >> return (builder, False)
        _ -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> "should not be like \"" <> T.unpack querySql <> "\". What happened?")
      restoreStateAndRun ctx QueryRunner {qRQueryString = querySql, ..}
    Nothing -> throwIO $ HE.QueryNotFound ("Query " <> qRQueryName <> "does not exist")
  where
    checkSources sources = do
      roles_m <- mapM (amIStream ctx) sources
      unless (and roles_m) $ do
        Log.warning $ "At least one of the streams/views do not exist: " <> Log.buildString' sources
        throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
#endif

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
    , querySources = V.fromList $ fst queryStreams
    , querySink = snd queryStreams
    }

-------------------------------------------------------------------------------

-- createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
-- createStreamWithShard client streamId shardName factor = do
--   S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
--   let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
--   void $ S.createStreamPartition client streamId (Just shardName) extrAttr
