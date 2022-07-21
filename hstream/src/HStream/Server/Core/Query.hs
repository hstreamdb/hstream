{-# LANGUAGE LambdaCase        #-}
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
import qualified Data.Vector                as V
import qualified Z.Data.CBytes              as CB

import           HStream.Server.Core.Common
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence as P
import           HStream.Server.Types
import qualified HStream.SQL.Codegen        as HSC
import           HStream.Utils

listQueries :: ServerContext -> IO [Query]
listQueries ServerContext{..} = do
  queries <- P.getQueries zkHandle
  return $ map hstreamQueryToQuery queries

getQuery :: ServerContext -> GetQueryRequest -> IO (Maybe Query)
getQuery ServerContext{..} GetQueryRequest{..} = do
  queries <- P.getQueries zkHandle
  return $ hstreamQueryToQuery <$>
    L.find (\P.PersistentQuery{..} ->
             cBytesToText queryId == getQueryRequestId) queries

terminateQueries :: ServerContext
                 -> TerminateQueriesRequest
                 -> IO (Either [CB.CBytes] TerminateQueriesResponse)
terminateQueries ctx@ServerContext{..} TerminateQueriesRequest{..} = do
  qids <- if terminateQueriesRequestAll
          then HM.keys <$> readMVar runningQueries
          else return . V.toList $ textToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate ctx (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then return (Left terminatedQids)
    else return (Right $ TerminateQueriesResponse (V.fromList $ cBytesToText <$> terminatedQids))

deleteQuery :: ServerContext -> DeleteQueryRequest -> IO ()
deleteQuery ServerContext{..} DeleteQueryRequest{..} =
  P.removeQuery (textToCBytes deleteQueryRequestId) zkHandle

--------------------------------------------------------------------------------
hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _ _) =
  Query
  { queryId          = cBytesToText queryId
  , queryStatus      = getPBStatus status
  , queryCreatedTime = createdTime
  , queryQueryText   = sqlStatement
  }
