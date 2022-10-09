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
import qualified HStream.Logger              as Log
import qualified HStream.MetaStore.Types     as M
import           HStream.Server.Core.Common  (deleteStoreStream)
import qualified HStream.Server.HStreamApi   as API
import qualified HStream.Server.MetaData     as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf (Empty)
import           HStream.Utils               (TaskStatus (..))

deleteView :: ServerContext -> T.Text -> Bool -> IO Empty
deleteView sc name checkIfExist = do
  atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
  deleteStoreStream sc (transToStreamName name) checkIfExist

getView :: ServerContext -> T.Text -> IO API.View
getView ServerContext{..} viewId = do
  query <- do
    viewQueries <- filter P.isViewQuery <$> M.listMeta zkHandle
    return $
      find (\P.PersistentQuery {..} -> queryId == viewId) viewQueries
  case query of
    Just q -> pure $ hstreamQueryToView q
    _      -> do
      Log.warning $ "Cannot Find View with ID: " <> Log.buildString (T.unpack viewId)
      throwIO $ ViewNotFound "View does not exist"

hstreamQueryToView :: P.PersistentQuery -> API.View
hstreamQueryToView (P.PersistentQuery _ sqlStatement createdTime (P.ViewQuery _ viewName schema) status _ _) =
  API.View {
      viewViewId = viewName
    , viewStatus = getPBStatus status
    , viewCreatedTime = createdTime
    , viewSql = sqlStatement
    , viewSchema = V.fromList $ T.pack <$> schema
    }
hstreamQueryToView _ = throw $ UnexpectedError "unexpected match in hstreamQueryToView."

listViews :: HasCallStack => ServerContext -> IO [API.View]
listViews ServerContext{..} = map hstreamQueryToView . filter P.isViewQuery <$> M.listMeta zkHandle

listViewNames :: ServerContext -> IO [T.Text]
listViewNames ServerContext{..} = do
  xs <- map hstreamQueryToView . filter P.isViewQuery <$> M.listMeta zkHandle
  pure (API.viewViewId <$> xs)
