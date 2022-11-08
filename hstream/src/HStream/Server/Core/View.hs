module HStream.Server.Core.View
  ( deleteView
  , getView
  , listViews
  , listViewNames
  ) where

import           Control.Exception           (throw, throwIO)
import qualified Data.HashMap.Strict         as HM
import           Data.IORef                  (atomicModifyIORef')
import           Data.List                   (find)
import qualified Data.Text                   as T
import qualified Data.Vector                 as V
import           GHC.Stack                   (HasCallStack)

import           HStream.Exception           (UnexpectedError (..),
                                              ViewNotFound (..))
import qualified HStream.Exception           as HE
import qualified HStream.Logger              as Log
import qualified HStream.MetaStore.Types     as M
import           HStream.Server.Core.Common  (handleQueryTerminate)
import qualified HStream.Server.HStreamApi   as API
import qualified HStream.Server.MetaData     as P
import           HStream.Server.Types
import           HStream.SQL.Codegen         (TerminationSelection (..))
import           HStream.ThirdParty.Protobuf (Empty (..))
import           HStream.Utils               (TaskStatus (..))

-- TODO: refactor this function after the meta data has been reorganized
deleteView :: ServerContext -> T.Text -> Bool -> IO Empty
deleteView sc@ServerContext{..} name checkIfExist = undefined
-- do
--   query <- do
--     viewQueries <- filter P.isViewQuery <$> M.listMeta metaHandle
--     return $ find ((== name) . P.getQuerySink) viewQueries
--   case query of
--     Just P.PersistentQuery{..} -> do
--       handleQueryTerminate sc (OneQuery queryId) >>= \case
--         [] -> throwIO $ HE.UnexpectedError "Failed to view related query for some unknown reason"
--         _  -> do
--           M.deleteMeta @P.PersistentQuery queryId Nothing metaHandle
--           atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
--           return Empty
--     Nothing -> if checkIfExist then return Empty
--                                else throwIO $ HE.ViewNotFound (name <> " does not exist")

getView :: ServerContext -> T.Text -> IO API.View
getView ServerContext{..} viewId = undefined
  -- query <- do
  --   viewQueries <- filter P.isViewQuery <$> M.listMeta metaHandle
  --   return $ find ((== viewId) . P.getQuerySink) viewQueries
  -- case query of
  --   Just q -> pure $ hstreamQueryToView q
  --   _      -> do
  --     Log.warning $ "Cannot Find View with Name: " <> Log.buildString (T.unpack viewId)
  --     throwIO $ ViewNotFound "View does not exist"

-- hstreamQueryToView :: P.PersistentQuery -> API.View
-- hstreamQueryToView (P.PersistentQuery _ sqlStatement createdTime (P.ViewQuery _ viewName) status _ _) =
--   API.View {
--       viewViewId = viewName
--     , viewStatus = getPBStatus status
--     , viewCreatedTime = createdTime
--     , viewSql = sqlStatement
--     , viewSchema = V.empty
--     }
-- hstreamQueryToView _ = throw $ UnexpectedError "unexpected match in hstreamQueryToView."

listViews :: HasCallStack => ServerContext -> IO [API.View]
listViews ServerContext{..} = undefined -- map hstreamQueryToView . filter P.isViewQuery <$> M.listMeta metaHandle

listViewNames :: ServerContext -> IO [T.Text]
listViewNames ServerContext{..} = undefined
  --  do
  -- xs <- map hstreamQueryToView . filter P.isViewQuery <$> M.listMeta metaHandle
  -- pure (API.viewViewId <$> xs)
