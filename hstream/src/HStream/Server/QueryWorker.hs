{-# LANGUAGE CPP #-}

module HStream.Server.QueryWorker where

#ifdef HStreamEnableSchema
import           HStream.Server.Core.QueryNew (listQueries, resumeQuery)
#else
import           HStream.Server.Core.Query    (listQueries, resumeQuery)
#endif

import qualified Control.Concurrent           as M
import qualified Data.HashMap.Strict          as HM
import qualified HStream.Server.HStreamApi    as API
import           HStream.Server.Types         (ServerContext (runningQueries),
                                               TaskManager (..))
import           HStream.Utils                (ResourceType (ResQuery))

-- TODO: modularize query worker(e.g. replace runningQueries with QueryWorker)
newtype QueryWorker = QueryWorker
  { ctx     :: ServerContext
  }

instance TaskManager QueryWorker where
  resourceType = const ResQuery
  listResources QueryWorker{..} = fmap API.queryId <$> listQueries ctx
  listRecoverableResources this@QueryWorker{..} = do
    qs <- M.readMVar (runningQueries ctx)
    rs <- listResources this
    return $ filter (not . (`HM.member` qs)) rs
  recoverTask QueryWorker{..} = resumeQuery ctx
