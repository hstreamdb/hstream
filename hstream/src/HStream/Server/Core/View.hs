module HStream.Server.Core.View
  ( deleteView
  , getView
  , listViews
  , createView
  , createView'
  ) where

import           Control.Exception             (throw, throwIO)
import qualified Data.HashMap.Strict           as HM
import           Data.IORef                    (atomicModifyIORef')
import qualified Data.Text                     as T
import           GHC.Stack                     (HasCallStack)

import           Control.Concurrent            (MVar)
import           Control.Monad                 (unless)
import           Data.Functor                  ((<&>))
import qualified Data.List                     as L
import           Data.Maybe                    (fromJust, isJust)
import           DiffFlow.Graph                (GraphBuilder)
import           DiffFlow.Types                (DataChangeBatch)
import           HStream.Exception             (ViewNotFound (..))
import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import           HStream.MetaStore.Types       (MetaStore (insertMeta))
import qualified HStream.MetaStore.Types       as M
import           HStream.Server.Core.Common    (handleQueryTerminate)
import           HStream.Server.Handler.Common (IdentifierRole (..),
                                                findIdentifierRole,
                                                handleCreateAsSelect)
import qualified HStream.Server.HStreamApi     as API
import qualified HStream.Server.MetaData       as P
import           HStream.Server.MetaData.Types (ViewInfo (viewName))
import           HStream.Server.Types
import           HStream.SQL                   (FlowObject)
import           HStream.SQL.Codegen           (HStreamPlan (..), In (..),
                                                Out (..), Row,
                                                TerminationSelection (..),
                                                streamCodegen)
import           HStream.ThirdParty.Protobuf   (Empty (..))
import           HStream.Utils                 (TaskStatus (..))

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
      qInfo <- handleCreateAsSelect sc
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
      throwIO $ HE.StreamNotFound $ "At least one of the streams/views do not exist: " <> T.pack (show sources)

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
    Nothing -> do unless checkIfExist $ throwIO $ HE.ViewNotFound (name <> " not found"); return Empty

getView :: ServerContext -> T.Text -> IO API.View
getView ServerContext{..} viewId = do
  M.getMeta @ViewInfo viewId metaHandle >>= \case
    Just vInfo -> hstreamViewToView metaHandle vInfo
    Nothing    -> do
      Log.warning $ "Cannot Find View with Name: " <> Log.buildString (T.unpack viewId)
      throwIO $ ViewNotFound "View does not exist"

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
    }
