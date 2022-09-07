{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Core.Query
  ( listQueries
  , getQuery
  , terminateQueries
  , deleteQuery

  , hstreamQueryToQuery
  ) where

import           Control.Concurrent
import qualified Data.HashMap.Strict        as HM
import qualified Data.List                  as L
import qualified Data.Text                  as T
import qualified Data.Vector                as V

import qualified HStream.MetaStore.Types    as M
import           HStream.Server.Core.Common
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData
import qualified HStream.Server.MetaData    as P
import           HStream.Server.Types
import qualified HStream.SQL.Codegen        as HSC
import           HStream.Utils

listQueries :: ServerContext -> IO [Query]
listQueries ServerContext{..} = do
  queries <- M.listMeta zkHandle
  return $ map hstreamQueryToQuery queries

getQuery :: ServerContext -> GetQueryRequest -> IO (Maybe Query)
getQuery ServerContext{..} GetQueryRequest{..} = do
  queries <- M.listMeta zkHandle
  return $ hstreamQueryToQuery <$>
    L.find (\P.PersistentQuery{..} -> queryId == getQueryRequestId) queries

terminateQueries :: ServerContext
                 -> TerminateQueriesRequest
                 -> IO (Either [T.Text] TerminateQueriesResponse)
terminateQueries ctx@ServerContext{..} TerminateQueriesRequest{..} = do
  qids <- if terminateQueriesRequestAll
          then HM.keys <$> readMVar runningQueries
          else return . V.toList $ terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate ctx (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then return (Left terminatedQids)
    else return (Right $ TerminateQueriesResponse (V.fromList terminatedQids))

deleteQuery :: ServerContext -> DeleteQueryRequest -> IO ()
deleteQuery ServerContext{..} DeleteQueryRequest{..} =
  M.deleteMeta @P.PersistentQuery deleteQueryRequestId Nothing zkHandle

--------------------------------------------------------------------------------
hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _ _) =
  Query
  { queryId          = queryId
  , queryStatus      = getPBStatus status
  , queryCreatedTime = createdTime
  , queryQueryText   = sqlStatement
  }
