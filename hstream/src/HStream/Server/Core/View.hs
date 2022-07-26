module HStream.Server.Core.View where

import qualified Data.HashMap.Strict         as HM
import           Data.IORef                  (atomicModifyIORef')
import qualified Data.Text                   as T
import qualified Data.Vector                 as V
import           GHC.Stack                   (HasCallStack)

import           HStream.Server.Core.Common  (deleteStoreStream)
import qualified HStream.Server.HStreamApi   as API
import qualified HStream.Server.Persistence  as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf (Empty)
import           HStream.Utils               (TaskStatus (..), cBytesToText)

deleteView :: ServerContext -> T.Text -> Bool -> IO Empty
deleteView sc name checkIfExist = do
  atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
  deleteStoreStream sc (transToStreamName name) checkIfExist

hstreamQueryToView :: P.PersistentQuery -> API.View
hstreamQueryToView (P.PersistentQuery _ sqlStatement createdTime (P.ViewQuery _ viewName schema) status _ _) =
  API.View { viewViewId = cBytesToText viewName
       , viewStatus = getPBStatus status
       , viewCreatedTime = createdTime
       , viewSql = sqlStatement
       , viewSchema = V.fromList $ T.pack <$> schema
       }
hstreamQueryToView _ = error "Impossible happened..."

listViews :: HasCallStack => ServerContext -> IO [API.View]
listViews ServerContext{..} = map hstreamQueryToView . filter P.isViewQuery <$> P.getQueries zkHandle
