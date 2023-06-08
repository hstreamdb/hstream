{-# LANGUAGE CPP #-}

module HStream.Server.Core.View
  ( deleteView
  , getView
  , listViews
  , executeViewQuery
  , executeViewQueryWithNamespace
  , createView
  , createView'
  , hstreamViewToView
  ) where

import           Control.Applicative           (liftA2)
import           Control.Concurrent.STM        (newTVarIO)
import           Control.Exception             (throw, throwIO)
import           Control.Monad                 (forM, forM_, unless, when)
import qualified Data.Aeson                    as Aeson
import           Data.Functor                  ((<&>))
import qualified Data.HashMap.Strict           as HM
import           Data.IORef                    (atomicModifyIORef', newIORef,
                                                readIORef)
import qualified Data.List                     as L
import           Data.Maybe                    (fromJust, isJust)
import qualified Data.Text                     as T
import qualified Data.Vector                   as V
import           GHC.Stack                     (HasCallStack)

import           HStream.Exception             (ViewNotFound (..))
import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import qualified HStream.MetaStore.Types       as M
import           HStream.Processing.Type       (SinkRecord (..))
import           HStream.Server.Core.Common    (modifySelect)
import           HStream.Server.Handler.Common (IdentifierRole (..),
                                                QueryRunner (..), amIView,
                                                createQueryAndRun,
                                                findIdentifierRole)
import qualified HStream.Server.HStore         as SH
import qualified HStream.Server.HStreamApi     as API
import qualified HStream.Server.MetaData       as P
import           HStream.Server.MetaData.Types (ViewInfo (viewName))
import           HStream.Server.Types
import           HStream.SQL                   (flowObjectToJsonObject,
                                                parseAndRefine)
import           HStream.ThirdParty.Protobuf   (Empty (..), Struct)
import           HStream.Utils                 (TaskStatus (..),
                                                jsonObjectToStruct,
                                                textToCBytes)
#ifdef HStreamUseV2Engine
import           DiffFlow.Graph                (GraphBuilder)
import           DiffFlow.Types                (DataChangeBatch)
import           HStream.SQL.Codegen           (HStreamPlan (..), In (..),
                                                Out (..), Row,
                                                TerminationSelection (..),
                                                streamCodegen)
#else
import qualified HStream.Processing.Processor  as HP
import qualified HStream.Processing.Stream     as HS
import           HStream.SQL.AST
import           HStream.SQL.Codegen.V1
import           HStream.SQL.Planner
import qualified HStream.Stats                 as Stats
#endif

#ifdef HStreamUseV2Engine
createView :: ServerContext -> T.Text -> IO P.ViewInfo
createView sc sql = do
  plan <- streamCodegen sql
  case plan of
    CreateViewPlan view ins out builder accumulation -> do
      createView' sc view ins out builder accumulation sql
    _ ->  throw $ HE.WrongExecutionPlan "Create query only support create view as select statements"

createView' :: ServerContext -> T.Text
  -> [In] -> Out -> GraphBuilder Row
  -> MVar (DataChangeBatch FlowObject Timestamp) -> T.Text
  -> IO ViewInfo
createView' sc@ServerContext{..} view ins out builder accumulation sql = do
  let sources = inStream <$> ins
      sink    = view
  roles_m <- mapM (findIdentifierRole sc) sources
  case all isJust roles_m of
    True -> do
      let relatedStreams = (sources, sink)
      qInfo <- createQueryAndRun sc
        sink (ins `zip` L.map fromJust roles_m) (out, RoleView)
        builder sql relatedStreams
      atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink accumulation hm, ()))
      let vInfo = P.ViewInfo{ viewName = view, viewQuery = qInfo }
      -- FIXME: this should be inserted as the same time as the query
      insertMeta view vInfo metaHandle
      return vInfo
    False  -> do
      Log.warning $ "At least one of the streams/views do not exist: "
        <> Log.buildString (show sources)
      -- FIXME: use another exception or find which resource doesn't exist
      throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)
#else
createView :: ServerContext -> T.Text -> T.Text -> IO P.ViewInfo
createView sc sql queryName = do
  plan <- streamCodegen sql
  case plan of
    CreateViewPlan srcs sink view builder persist -> do
      rSQL <- parseAndRefine sql
      createView' sc view srcs sink builder persist sql rSQL queryName
    _ ->  throw $ HE.WrongExecutionPlan "Create query only support create view as select statements"

createView' :: ServerContext
            -> T.Text
            -> [T.Text]
            -> T.Text
            -> HP.TaskBuilder
            -> Persist
            -> T.Text
            -> RSQL
            -> T.Text
            -> IO ViewInfo
createView' sc@ServerContext{..} view srcs sink builder persist sql rSQL queryName = do
  roles_m <- mapM (findIdentifierRole sc) srcs
  case all isJust roles_m of
    True -> do
      -- TODO: Support joining between streams and views
      case all (== Just RoleStream) roles_m of
        False -> do
          Log.warning "CREATE VIEW only supports sources of stream type"
          throwIO $ HE.InvalidSqlStatement "CREATE VIEW only supports sources of stream type"
        True  -> do
          let relatedStreams = (srcs, sink)
          vInfo <- P.createInsertViewQueryInfo queryName sql rSQL relatedStreams view metaHandle
          consumerClosed <- newTVarIO False
          createQueryAndRun sc QueryRunner {
              qRTaskBuilder = builder
            , qRQueryName   = queryName
            , qRQueryString = sql
            , qRWhetherToHStore = False
            , qRQuerySources = srcs
            , qRConsumerClosed = consumerClosed
            }
          let accumulation = L.head (snd persist)
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert view accumulation hm, ()))
          return vInfo
    False  -> do
      Log.warning $ "At least one of the streams/views do not exist: "
        <> Log.buildString (show srcs)
      throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show srcs)
#endif

-- TODO: refactor this function after the meta data has been reorganized
deleteView :: ServerContext -> T.Text -> Bool -> IO Empty
deleteView sc@ServerContext{..} name checkIfExist = do
  M.getMeta @P.ViewInfo name metaHandle >>= \case
    Just P.ViewInfo{viewQuery = P.QueryInfo {..}} -> do
      M.getMeta @P.QueryStatus queryId metaHandle >>= \case
        Nothing -> throwIO $ HE.QueryNotFound $ "Query " <> queryId <> " associated with view " <> name <> " is not found"
        Just P.QueryStatus{..} -> when (queryState /= Terminated) $ do
           throwIO $ HE.QueryNotTerminated queryId
      P.deleteViewQuery name queryId metaHandle
      atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
      Stats.view_stat_erase scStatsHolder (textToCBytes name)
      return Empty
    Nothing -> do unless checkIfExist $ throwIO $ HE.ViewNotFound name; return Empty

getView :: ServerContext -> T.Text -> IO API.View
getView ServerContext{..} viewId = do
  M.getMeta @ViewInfo viewId metaHandle >>= \case
    Just vInfo -> hstreamViewToView metaHandle vInfo
    Nothing    -> do
      Log.warning $ "Cannot Find View with Name: " <> Log.buildString (T.unpack viewId)
      throwIO $ ViewNotFound viewId

executeViewQuery :: ServerContext -> T.Text -> IO (V.Vector Struct)
executeViewQuery ctx sql = executeViewQueryWithNamespace ctx sql ""

executeViewQueryWithNamespace :: ServerContext -> T.Text -> T.Text -> IO (V.Vector Struct)
executeViewQueryWithNamespace sc@ServerContext{..} sql namespace = parseAndRefine sql >>= \case
  RQSelect select_imm -> do
    -- FIXME: Very tricky hack, invasive to the SQL Planner.
    -- 1. the immediate query to the view
    let select_imm_namespaced = modifySelect namespace select_imm
        relationExpr_imm = decouple select_imm_namespaced
    taskName_imm <- genTaskName
    (_,_view_names,_,_) <- elabRelationExpr taskName_imm Nothing relationExpr_imm
    -- FIXME: immediate select from [views join views/streams]?
    -- FIXME: L.head is partial
    let view_name = L.head _view_names

    -- 2. the original query when creating the view
    (RQCreate (RCreateView _ select_v))
      <- M.getMeta @ViewInfo view_name metaHandle >>= \case
        Nothing    -> throwIO $ ViewNotFound view_name
        Just vInfo -> return $ P.queryRefinedAST (P.viewQuery vInfo)
    let relationExpr_v = decouple select_v
    -- 3.1 scan for HAVING
    let trans_m_having =
          case scanRelationExpr (\r -> case r of
                                    Filter{} -> True
                                    _        -> False
                                ) Nothing relationExpr_v of
            (Nothing                     , _) -> Nothing -- no WHERE and HAVING
            (Just (Filter r' hav_scalar'), _) -> -- can not determine WHERE or HAVING
              case scanRelationExpr (\r -> case r of
                                        Reduce{} -> True
                                        _        -> False
                                    ) Nothing r' of
                (Nothing, _) -> Nothing -- only one Filter but no Reduce, it is WHERE
                (Just _ , _) -> Just (\r -> Filter r hav_scalar') -- both Filter and Reduce, it is HAVING
    -- 3.2 scan for PROJECT
    let trans_m_project =
          case scanRelationExpr (\r -> case r of
                                    Project{} -> True
                                    _         -> False
                                ) Nothing relationExpr_v of
            (Nothing                        , _) -> Nothing
            (Just (Project _ tups asterisks), _) -> Just (\r -> Project r tups asterisks)
    -- 4 insert HAVING and PROJECT into the immediate query
    let (_, relationExpr_imm_new) =
          scanRelationExpr (\r -> case r of
                                    CrossJoin{}       -> True
                                    LoopJoinOn{}      -> True
                                    LoopJoinUsing{}   -> True
                                    LoopJoinNatural{} -> True
                                    StreamScan{}      -> True
                                    StreamRename{}    -> True
                                    _                 -> False
                           ) -- scan for the end of the FROM part
                           (liftA2 (.) trans_m_project trans_m_having) -- insert the HAVING and PROJECT part
                           relationExpr_imm
    -- 5. generate plan for the modified immediate query
    (_builder, sources, sink, persist)
      <- elabRelationExpr taskName_imm Nothing relationExpr_imm_new
    let builder = HS.build _builder
    -- 6. run the plan
    isViews <- mapM (amIView sc) sources
    case and isViews of
      False -> do
        Log.warning "View not found"
        throwIO $ HE.ViewNotFound "View not found"
      True  -> do
        hm <- readIORef P.groupbyStores
        mats <- forM sources $ \source -> do
          case HM.lookup source hm of
            Nothing -> do
              Log.warning $ "View " <> Log.buildString (T.unpack source)
                          <> " does not exist in memory. Try restarting the server or reporting this as a bug."
              throwIO $ HE.ViewNotFound $ "View " <> source <> " does not exist in memory"
            Just mat  -> return mat
        sinkRecords_m <- newIORef []
        let sinkConnector = SH.memorySinkConnector sinkRecords_m
        HP.runImmTask (sources `zip` mats) sinkConnector builder () () Just Just
        sinkRecords <- readIORef sinkRecords_m
        let flowObjects = L.map (fromJust . Aeson.decode . snkValue) sinkRecords :: [FlowObject]
        forM_ sources $ \viewName -> Stats.view_stat_add_total_execute_queries scStatsHolder (textToCBytes viewName) 1
        return . V.fromList $ jsonObjectToStruct . flowObjectToJsonObject
                            <$> flowObjects
  _ -> throw $ HE.InvalidSqlStatement "Invalid SQL statement for running view query"

listViews :: HasCallStack => ServerContext -> IO [API.View]
listViews ServerContext{..} = mapM (hstreamViewToView metaHandle) =<< M.listMeta metaHandle

hstreamViewToView :: M.MetaHandle -> ViewInfo -> IO API.View
hstreamViewToView h P.ViewInfo{viewQuery = P.QueryInfo{..},..} = do
  state <- M.getMeta @P.QueryStatus queryId h <&> maybe Unknown P.queryState
  return API.View {
      viewViewId = viewName
    , viewStatus = getPBStatus state
    , viewCreatedTime = queryCreatedTime
    , viewSchema = mempty
    , viewSql = querySql
    , viewQueryName = queryId
    }
