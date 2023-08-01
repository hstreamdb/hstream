{-# LANGUAGE CPP               #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- This module is only compiled when 'hstream_enable_schema' is disabled.
module HStream.Server.Core.Query (
#ifndef HStreamEnableSchema
    executeQuery

  , createQuery
  , createQueryWithNamespace
  , listQueries
  , getQuery
  , deleteQuery

  , hstreamQueryToQuery

  , resumeQuery

  -- re-export
  , terminateQuery
#endif
  ) where

#ifndef HStreamEnableSchema
import           Control.Concurrent.STM           (newTVarIO)
import           Control.Exception                (SomeException, throw,
                                                   throwIO, try)
import           Control.Monad
import           Data.Foldable
import           Data.Functor                     ((<&>))
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import           Data.Maybe                       (fromJust, isJust, isNothing)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Network.GRPC.HighLevel.Generated (GRPCIOError)
import           Proto3.Suite.Class               (def)
import qualified Proto3.Suite.JSONPB              as PB

import qualified HStream.Exception                as HE
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.Core.Common
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.View         as Core
import           HStream.Server.Handler.Common
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Types
import           HStream.SQL
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import qualified HStream.Utils.Aeson              as AesonComp
#ifdef HStreamUseV2Engine
import           DiffFlow.Types                   (DataChange (..),
                                                   DataChangeBatch (..),
                                                   emptyDataChangeBatch)
import           HStream.Server.ConnectorTypes    hiding (StreamName, Timestamp)
import qualified HStream.SQL.Codegen.V2           as HSC
#else
import           Data.IORef
import           HStream.Processing.Connector
import           HStream.Processing.Type
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
      validateNameAndThrow ResView view
      P.ViewInfo{viewQuery=P.QueryInfo{..}} <-
        Core.createView' sc view ins out builder accumulation commandQueryStmtText
      pure $ API.CommandQueryResponse (mkVectorStruct queryId "view_query_id")
#else
    SelectPlan {} -> discard "ExecuteViewQuery"
    CreateViewPlan sources sink view builder persist -> discard "CreateQuery"
#endif
    ExplainPlan plan -> pure $ API.CommandQueryResponse (mkVectorStruct plan "explain")
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False
      pure (CommandQueryResponse V.empty)
    ResumePlan (ResumeObjectConnector name) -> do
      IO.recoverTask scIOWorker name
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
    InsertBySelectPlan {} -> discard "CreateQuery"
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

-- NOTE: createQueryWithNameSpace may be modified in future.
createQuery :: HasCallStack => ServerContext -> CreateQueryRequest -> IO Query
createQuery sc req = createQueryWithNamespace' sc req ""

createQueryWithNamespace ::
  HasCallStack => ServerContext -> CreateQueryWithNamespaceRequest -> IO Query
createQueryWithNamespace
  sc CreateQueryWithNamespaceRequest {..} =
  createQueryWithNamespace' sc CreateQueryRequest{
      createQueryRequestSql = createQueryWithNamespaceRequestSql
    , createQueryRequestQueryName = createQueryWithNamespaceRequestQueryName } createQueryWithNamespaceRequestNamespace

createQueryWithNamespace' ::
  HasCallStack => ServerContext -> CreateQueryRequest -> T.Text -> IO Query
createQueryWithNamespace'
  sc@ServerContext {..} CreateQueryRequest {..} namespace =
  M.checkMetaExists @P.QueryInfo createQueryRequestQueryName metaHandle >>= \case
    True  -> throwIO $ HE.QueryExists createQueryRequestQueryName
    False -> do
#ifdef HStreamUseV2Engine
      plan <- streamCodegen createQueryRequestSql
      case plan of
        CreateBySelectPlan stream ins out builder factor -> do
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
      parseAndRefine createQueryRequestSql >>= \rSQL -> case rSQL of
        RQCreate (RCreateAs stream select rOptions) ->
          hstreamCodegen (RQCreate (RCreateAs (namespace <> stream)
                                              (modifySelect namespace select)
                                              rOptions)) >>= \case
            CreateBySelectPlan sources sink builder factor persist -> do
              -- validate names
              mapM_ (validateNameAndThrow ResStream) sources
              validateNameAndThrow ResStream sink
              -- check source streams
              roles_m <- mapM (findIdentifierRole sc) sources
              unless (all isJust roles_m) $ do
                Log.warning $ "At least one of the streams/views do not exist: "
                    <> Log.buildString (show sources)
                throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
              unless (all (== Just RoleStream) roles_m) $ do
                Log.warning "CREATE STREAM only supports sources of stream type"
                throwIO $ HE.InvalidSqlStatement "CREATE STREAM only supports sources of stream type"
              -- check & prepare sink stream
              S.doesStreamExist scLDClient (transToStreamName sink) >>= \case
                True  -> do
                  Log.warning $ "Sink stream already exists: " <> Log.buildString (show sink)
                  throwIO $ HE.StreamExists sink
                False -> do
                  Core.createStream sc def {
                    streamStreamName = sink
                  , streamReplicationFactor = 1
                  , streamBacklogDuration = 7 * 24 * 3600
                  , streamShardCount = 1
                  }
              -- update metadata
              let relatedStreams = (sources, sink)
              qInfo <- P.createInsertQueryInfo createQueryRequestQueryName createQueryRequestSql relatedStreams rSQL serverID metaHandle
              -- run core task
              consumerClosed <- newTVarIO False
              createQueryAndRun sc QueryRunner {
                  qRTaskBuilder = builder
                , qRQueryName   = createQueryRequestQueryName
                , qRQueryString = createQueryRequestSql
                , qRWhetherToHStore = True
                , qRQuerySources    = sources
                , qRConsumerClosed  = consumerClosed
                }
              hstreamQueryToQuery metaHandle qInfo
            _ -> throw $ HE.WrongExecutionPlan "Create query only support create stream/view <name> as select statements"

        RQInsert (RInsertSel streamName rSel) -> do
          (hstreamCodegen $ RQInsert (RInsertSel
            (namespace <> streamName)
            (modifySelect namespace rSel))
            ) >>= \case
            InsertBySelectPlan srcs sink builder persist -> do
              -- validate names
              traverse_ (validateNameAndThrow ResStream) (sink : srcs)
              -- find all roles
              rolesM <- mapM (\x -> ((,) x) <$> findIdentifierRole sc x) srcs
              let notExistNames = filter (\(_, x) -> isNothing x) rolesM
              -- FIXME: Currently, we can use `L.head` here because we does not have const query (select without srcs) now.
              --        This check does not work as it should work, since we can construction many conditions to do evil.
              when (not (null srcs) && sink == head srcs) $ do
                Log.warning "Insert by Select: Can not insert by select the sink stream itself"
                throwIO $ HE.InvalidSqlStatement "Insert by Select: Can not insert by select the sink stream itself"
              when (notExistNames /= []) $ do
                Log.warning $ "Insert by Select: Streams not found: " <> Log.buildString (show srcs)
                throwIO . HE.StreamNotFound $ "Streams not found: " <> T.pack (show srcs)
              when (not $ all (\(_, x) -> x == Just RoleStream) rolesM) $ do
                Log.warning "Insert by Select only supports all sources of same resource type STREAM"
                throw $ HE.InvalidSqlStatement "Insert by Select only supports all sources of same resource type STREAM"
              -- check sink stream
              foundSink <- S.doesStreamExist scLDClient (transToStreamName sink)
              when (not foundSink) $ do
                Log.warning $ "Insert by Select: Stream not found: " <> Log.buildString (show streamName)
                throw $ HE.StreamNotFound $ "Stream " <> streamName <> " not found"
              -- update metadata
              qInfo <- P.createInsertQueryInfo createQueryRequestQueryName createQueryRequestSql
                (srcs, sink)
                rSQL serverID metaHandle
              -- run core task
              consumerClosed <- newTVarIO False
              createQueryAndRun sc QueryRunner
                { qRTaskBuilder = builder
                , qRQueryName   = createQueryRequestQueryName
                , qRQueryString = createQueryRequestSql
                , qRWhetherToHStore = True
                , qRQuerySources    = srcs
                , qRConsumerClosed  = consumerClosed
                }
              hstreamQueryToQuery metaHandle qInfo
            _ -> throw $ HE.WrongExecutionPlan "Insert by Select query only supports `INSERT INTO <stream_name> SELECT FROM STREAM`"

        RQCreate (RCreateView view select) ->
          hstreamCodegen (RQCreate (RCreateView (namespace <> view)
                                                (modifySelect namespace select)
                                   )) >>= \case
            CreateViewPlan sources sink view builder persist -> do
              validateNameAndThrow ResView view
              Core.createView' sc view sources sink builder persist createQueryRequestSql rSQL createQueryRequestQueryName
              >>= hstreamViewToQuery metaHandle
            _ -> throw $ HE.WrongExecutionPlan "Create query only support create stream/view <name> as select statements"
        _ -> throw $ HE.WrongExecutionPlan "Create query only support create stream/view <name> as select statements"
#endif

listQueries :: ServerContext -> IO [Query]
listQueries ServerContext{..} = do
  queries <- M.listMeta metaHandle
  mapM (hstreamQueryToQuery metaHandle) queries

getQuery :: ServerContext -> T.Text -> IO (Maybe Query)
getQuery ServerContext{..} qid = do
  queries <- M.listMeta metaHandle
  hstreamQueryToQuery metaHandle `traverse`
    L.find (\P.QueryInfo{..} -> queryId == qid) queries

deleteQuery :: HasCallStack => ServerContext -> DeleteQueryRequest -> IO ()
deleteQuery ServerContext{..} DeleteQueryRequest{..} = do
  getMeta @P.QueryStatus deleteQueryRequestId metaHandle >>= \case
    Nothing -> throwIO $ HE.QueryNotFound deleteQueryRequestId
    Just P.QueryStatus{..} -> when (queryState /= Terminated && queryState /= Aborted) $
      throwIO $ HE.QueryNotTerminated deleteQueryRequestId
  getMeta @P.QVRelation deleteQueryRequestId metaHandle >>= \case
    Nothing -> do
      -- do deletion
      -- Note: do not forget to delete the stream for changelog
      S.removeStream scLDClient (transToTempStreamName deleteQueryRequestId)
      P.deleteQueryInfo deleteQueryRequestId metaHandle
      Stats.connector_stat_erase scStatsHolder (textToCBytes deleteQueryRequestId)
    Just P.QVRelation{..} -> throwIO $ HE.FoundAssociatedView qvRelationViewName

----
-- Warning: may throw exceptions if restoration fails
#ifndef HStreamUseV2Engine
resumeQuery :: ServerContext -> T.Text -> IO ()
resumeQuery ctx@ServerContext{..} qRQueryName = do
  getMeta @P.QueryStatus qRQueryName metaHandle >>= \case
    Nothing -> throwIO $ HE.QueryNotFound ("Query " <> qRQueryName <> " does not exist")
    Just P.QueryAborted -> return ()
    -- NOTE: We don't have the state update from running to aborted
    --     , so if the method is called it means the node was down
    Just P.QueryRunning -> return ()
    Just P.QueryTerminated -> throwIO $ HE.QueryAlreadyTerminated qRQueryName
    Just state -> throwIO $ HE.QueryNotAborted (T.pack $ show state)
  getMeta @P.QueryInfo qRQueryName metaHandle >>= \case
    Just P.QueryInfo{..} -> do
      (qRTaskBuilder, qRWhetherToHStore, qRQuerySources) <- hstreamCodegen queryRefinedAST >>= \case
        CreateBySelectPlan sources _sink builder _ _ -> checkSources sources >> return (builder, True, sources)
        CreateViewPlan sources _sink view builder persist  -> do
          checkSources sources
          let accumulation = L.head (snd persist)
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert view accumulation hm, ()))
          return (builder, False, sources)
        _ -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> " should not be like \"" <> T.unpack querySql <> "\". What happened?")
      qRConsumerClosed <- newTVarIO False
      restoreStateAndRun ctx QueryRunner {qRQueryString = querySql, ..}
    Nothing -> throwIO $ HE.QueryNotFound ("Query " <> qRQueryName <> " does not exist")
  where
    checkSources sources = do
      roles_m <- mapM (amIStream ctx) sources
      unless (and roles_m) $ do
        Log.warning $ "At least one of the streams/views do not exist: " <> Log.buildString' sources
        M.updateMeta qRQueryName P.QueryTerminated Nothing metaHandle -- FIXME: a better state to indicate the error result?
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
    , queryStatus = getPBStatus state
    , queryCreatedTime = queryCreatedTime
    , queryQueryText = querySql
    , querySources = V.fromList $ fst queryStreams
    , queryResultName = snd queryStreams
    , queryType = getQueryType queryType
    , queryNodeId = workerNodeId
    }

hstreamViewToQuery :: M.MetaHandle -> P.ViewInfo -> IO API.Query
hstreamViewToQuery h P.ViewInfo{viewQuery = P.QueryInfo{..},..} = do
  state <- M.getMeta @P.QueryStatus queryId h <&> maybe Unknown P.queryState
  return API.Query
    { queryId = queryId
    , queryStatus = getPBStatus state
    , queryCreatedTime = queryCreatedTime
    , queryQueryText = querySql
    , querySources = V.fromList $ fst queryStreams
    , queryResultName = viewName
    , queryType = getQueryType queryType
    , queryNodeId = workerNodeId
    }

-------------------------------------------------------------------------------

-- createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
-- createStreamWithShard client streamId shardName factor = do
--   S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
--   let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
--   void $ S.createStreamPartition client streamId (Just shardName) extrAttr
#endif
