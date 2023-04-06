{-# LANGUAGE CPP #-}

module HStream.Server.Core.View
  ( deleteView
  , getView
  , listViews
  , executeViewQuery
  , createView
  , createView'
  ) where

import           Control.Exception             (throw, throwIO)
import           Control.Monad                 (unless)
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
import           HStream.MetaStore.Types       (MetaStore (insertMeta))
import qualified HStream.MetaStore.Types       as M
import           HStream.Processing.Type       (SinkRecord (..))
import           HStream.Server.Core.Common    (handleQueryTerminate)
import           HStream.Server.Handler.Common (IdentifierRole (..),
                                                QueryRunner (..), amIView,
                                                createQueryAndRun,
                                                findIdentifierRole)
import qualified HStream.Server.HStore         as SH
import qualified HStream.Server.HStreamApi     as API
import qualified HStream.Server.MetaData       as P
import           HStream.Server.MetaData.Types (ViewInfo (viewName))
import           HStream.Server.Types
import           HStream.SQL                   (FlowObject,
                                                flowObjectToJsonObject)
import           HStream.ThirdParty.Protobuf   (Empty (..), Struct)
import           HStream.Utils                 (TaskStatus (..),
                                                jsonObjectToStruct,
                                                newRandomText)
#ifdef HStreamUseV2Engine
import           DiffFlow.Graph                (GraphBuilder)
import           DiffFlow.Types                (DataChangeBatch)
import           HStream.SQL.Codegen           (HStreamPlan (..), In (..),
                                                Out (..), Row,
                                                TerminationSelection (..),
                                                streamCodegen)
#else
import qualified HStream.Processing.Processor  as HP
import           HStream.SQL.Codegen.V1
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
createView :: ServerContext -> T.Text -> IO P.ViewInfo
createView sc sql = do
  plan <- streamCodegen sql
  case plan of
    CreateViewPlan srcs sink view builder persist -> do
      createView' sc view srcs sink builder persist sql
    _ ->  throw $ HE.WrongExecutionPlan "Create query only support create view as select statements"

createView' :: ServerContext
            -> T.Text
            -> [T.Text]
            -> T.Text
            -> HP.TaskBuilder
            -> Persist
            -> T.Text
            -> IO ViewInfo
createView' sc@ServerContext{..} view srcs sink builder persist sql = do
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
          queryId <- newRandomText 10
          qInfo <- createQueryAndRun sc QueryRunner {
              qRTaskBuilder = builder
            , qRQueryName   = queryId
            , qRQueryString = sql
            , qRWhetherToHStore = False }
            relatedStreams
          let accumulation = L.head (snd persist)
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert view accumulation hm, ()))
          let vInfo = P.ViewInfo{ viewName = view, viewQuery = qInfo }
          -- FIXME: this should be inserted as the same time as the query
          insertMeta view vInfo metaHandle
          return vInfo
    False  -> do
      Log.warning $ "At least one of the streams/views do not exist: "
        <> Log.buildString (show srcs)
      throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show srcs)
#endif

-- TODO: refactor this function after the meta data has been reorganized
deleteView :: ServerContext -> T.Text -> Bool -> IO Empty
deleteView sc@ServerContext{..} name checkIfExist = do
  M.getMeta @ViewInfo name metaHandle >>= \case
    Just P.ViewInfo{viewQuery = P.QueryInfo {..}} -> do
      handleQueryTerminate sc (OneQuery queryId) >>= \case
        [] -> throwIO $ HE.UnexpectedError "Failed to view related query for some unknown reason"
        _  -> do
          M.metaMulti [ M.deleteMetaOp @P.QueryInfo queryId Nothing metaHandle
                      , M.deleteMetaOp @ViewInfo name Nothing metaHandle] metaHandle
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
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
executeViewQuery sc@ServerContext{..} sql = do
  plan <- streamCodegen sql
  case plan of
    SelectPlan sources sink builder persist (groupBykeys, keysAdded) -> do
      isViews <- mapM (amIView sc) sources
      case and isViews of
        False -> do
          Log.warning "Can not perform non-pushing SELECT on streams."
          throwIO $ HE.InvalidSqlStatement "Can not perform non-pushing SELECT on streams."
        True  -> do
          hm <- readIORef P.groupbyStores
          let mats = L.map (hm HM.!) sources
          sinkRecords_m <- newIORef []
          let sinkConnector = SH.memorySinkConnector sinkRecords_m
          HP.runImmTask (sources `zip` mats) sinkConnector builder () () Just Just
          sinkRecords <- readIORef sinkRecords_m
          let flowObjects = L.map (fromJust . Aeson.decode . snkValue) sinkRecords :: [FlowObject]
          let res =
                if L.null groupBykeys
                then flowObjects
                else
                  let compactedRes =
                        L.foldl'
                          (
                            \m fo ->
                              let values = L.map (fo HM.!) groupBykeys
                               in  HM.insert values fo m
                          )
                          HM.empty
                          flowObjects
                   in L.map (HM.filterWithKey (\ k _ -> L.notElem k keysAdded)) (HM.elems compactedRes)
          return . V.fromList $ jsonObjectToStruct . flowObjectToJsonObject
                              <$> res
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
