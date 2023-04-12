module HStream.Server.QueryWorker where

import           HStream.Server.Core.Query (listQueries, resumeQuery)
import qualified HStream.Server.HStreamApi as API
import           HStream.Server.Types      (ServerContext, TaskManager (..))
import           HStream.Utils             (ResourceType (ResQuery))

-- TODO: modularize query worker(e.g. replace runningQueries with QueryWorker)
newtype QueryWorker = QueryWorker
  { ctx     :: ServerContext
  }

instance TaskManager QueryWorker where
  resourceType = const ResQuery
  listResources QueryWorker{..} = fmap API.queryId <$> listQueries ctx
  recoverTask QueryWorker{..} = resumeQuery ctx
