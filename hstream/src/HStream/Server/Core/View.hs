{-# LANGUAGE CPP #-}

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
import           HStream.ThirdParty.Protobuf   (Empty (..))
import           HStream.Utils                 (TaskStatus (..), newRandomText)
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
      let relatedStreams = (srcs, sink)
      queryId <- newRandomText 10
      qInfo <- handleCreateAsSelect sc builder queryId sql relatedStreams False -- Do not write to any sink stream
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
