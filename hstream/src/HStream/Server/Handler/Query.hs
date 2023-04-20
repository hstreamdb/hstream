{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module HStream.Server.Handler.Query
  ( -- * For grpc-haskell
    executeQueryHandler
  , terminateQueryHandler
  , getQueryHandler
  , listQueriesHandler
  , deleteQueryHandler
  , resumeQueryHandler
  , createQueryHandler
  , createQueryWithNamespaceHandler
    -- * For hs-grpc-server
  , handleExecuteQuery
  , handleCreateQuery
  , handleCreateQueryWithNamespace
  , handleListQueries
  , handleGetQuery
  , handleTerminateQuery
  , handleDeleteQuery
  , handleResumeQuery
  ) where


import           Control.Exception                (Handler (..), catches,
                                                   throwIO)
import           Control.Monad                    (unless)
import qualified Data.ByteString.Char8            as BS
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import qualified HsGrpc.Server.Types              as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (lookupResource')
import qualified HStream.Server.Core.Query        as Core
import           HStream.Server.Exception         (defaultExHandlers,
                                                   defaultHandlers)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Types
import           HStream.Server.Validation
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.Stats                    (query_stat_add_queries_alive,
                                                   query_stat_add_queries_terminated)
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------

-- Other sqls, called in 'sqlAction'
executeQueryHandler :: ServerContext
                    -> ServerRequest 'Normal API.CommandQuery API.CommandQueryResponse
                    -> IO (ServerResponse 'Normal API.CommandQueryResponse)
executeQueryHandler sc (ServerNormalRequest _metadata req) =
  queryExceptionHandle $ returnResp =<< Core.executeQuery sc req

handleExecuteQuery :: ServerContext -> G.UnaryHandler API.CommandQuery API.CommandQueryResponse
handleExecuteQuery sc _ req = catchQueryEx $ Core.executeQuery sc req

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.CreateQueryRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
createQueryHandler ctx@ServerContext{scStatsHolder} (ServerNormalRequest _metadata req@API.CreateQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Create Query Request with statement: " <> Log.build createQueryRequestSql
             <> "and query name: " <> Log.build createQueryRequestQueryName
    validateCreateQuery req
    validateQueryAllocation ctx createQueryRequestQueryName
    res <- Core.createQuery ctx req
    query_stat_add_queries_alive scStatsHolder "alive" 1
    returnResp res

handleCreateQuery
  :: ServerContext -> G.UnaryHandler API.CreateQueryRequest API.Query
handleCreateQuery ctx@ServerContext{scStatsHolder} _ req@API.CreateQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Create Query Request with statement: " <> Log.build createQueryRequestSql
           <> "and query name: " <> Log.build createQueryRequestQueryName
  validateCreateQuery req
  validateQueryAllocation ctx createQueryRequestQueryName
  res <- Core.createQuery ctx req
  query_stat_add_queries_alive scStatsHolder "alive" 1
  return res

createQueryWithNamespaceHandler
  :: ServerContext
  -> ServerRequest 'Normal API.CreateQueryWithNamespaceRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
createQueryWithNamespaceHandler ctx@ServerContext{scStatsHolder} (ServerNormalRequest _metadata req@API.CreateQueryWithNamespaceRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Create Query Request with statement: " <> Log.build createQueryWithNamespaceRequestSql
             <> "and query name: " <> Log.build createQueryWithNamespaceRequestQueryName
    validateCreateQueryWithNamespace req
    validateQueryAllocation ctx createQueryWithNamespaceRequestQueryName
    res <- Core.createQueryWithNamespace ctx req
    query_stat_add_queries_alive scStatsHolder "alive" 1
    returnResp res

handleCreateQueryWithNamespace
  :: ServerContext -> G.UnaryHandler API.CreateQueryWithNamespaceRequest API.Query
handleCreateQueryWithNamespace ctx@ServerContext{scStatsHolder} _ req@API.CreateQueryWithNamespaceRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Create Query Request with statement: " <> Log.build createQueryWithNamespaceRequestSql
           <> "and query name: " <> Log.build createQueryWithNamespaceRequestQueryName
  validateCreateQueryWithNamespace req
  validateQueryAllocation ctx createQueryWithNamespaceRequestQueryName
  res <- Core.createQueryWithNamespace ctx req
  query_stat_add_queries_alive scStatsHolder "alive" 1
  return res

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ListQueriesRequest API.ListQueriesResponse
  -> IO (ServerResponse 'Normal API.ListQueriesResponse)
listQueriesHandler ctx (ServerNormalRequest _metadata _) = queryExceptionHandle $ do
  Log.debug "Receive List Query Request"
  Core.listQueries ctx >>= returnResp . (API.ListQueriesResponse . V.fromList)

handleListQueries
  :: ServerContext -> G.UnaryHandler API.ListQueriesRequest API.ListQueriesResponse
handleListQueries ctx _ _ = catchQueryEx $ do
  Log.debug "Receive List Query Request"
  API.ListQueriesResponse . V.fromList <$> Core.listQueries ctx

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.GetQueryRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
getQueryHandler ctx (ServerNormalRequest _metadata req@API.GetQueryRequest{..}) =
  queryExceptionHandle $ do
    validateNameAndThrow ResQuery getQueryRequestId
    validateQueryAllocation ctx getQueryRequestId
    Log.debug $ "Receive Get Query Request. "
             <> "Query ID: " <> Log.build getQueryRequestId
    returnResp =<< Core.getQuery ctx req

handleGetQuery :: ServerContext -> G.UnaryHandler API.GetQueryRequest API.Query
handleGetQuery ctx _ req@API.GetQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Get Query Request. "
           <> "Query ID: " <> Log.build getQueryRequestId
  validateNameAndThrow ResQuery getQueryRequestId
  validateQueryAllocation ctx getQueryRequestId
  Core.getQuery ctx req

terminateQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.TerminateQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
terminateQueryHandler ctx@ServerContext{scStatsHolder} (ServerNormalRequest _metadata API.TerminateQueryRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueryRequestQueryId)
  validateNameAndThrow ResQuery terminateQueryRequestQueryId
  validateQueryAllocation ctx terminateQueryRequestQueryId
  Core.terminateQuery ctx terminateQueryRequestQueryId
  query_stat_add_queries_alive scStatsHolder "alive" (-1)
  query_stat_add_queries_terminated scStatsHolder "terminated" 1
  returnResp Empty

handleTerminateQuery
  :: ServerContext -> G.UnaryHandler API.TerminateQueryRequest Empty
handleTerminateQuery ctx@ServerContext{scStatsHolder} _ API.TerminateQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueryRequestQueryId)
  validateNameAndThrow ResQuery terminateQueryRequestQueryId
  validateQueryAllocation ctx terminateQueryRequestQueryId
  Core.terminateQuery ctx terminateQueryRequestQueryId
  query_stat_add_queries_alive scStatsHolder "alive" (-1)
  query_stat_add_queries_terminated scStatsHolder "terminated" 1
  return Empty

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ctx@ServerContext{scStatsHolder} (ServerNormalRequest _metadata req@API.DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.build deleteQueryRequestId
    validateNameAndThrow ResQuery deleteQueryRequestId
    validateQueryAllocation ctx deleteQueryRequestId
    Core.deleteQuery ctx req
    query_stat_add_queries_terminated scStatsHolder "terminated" (-1)
    returnResp Empty

handleDeleteQuery :: ServerContext -> G.UnaryHandler API.DeleteQueryRequest Empty
handleDeleteQuery ctx@ServerContext{scStatsHolder}_ req@API.DeleteQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Delete Query Request. "
           <> "Query ID: " <> Log.build deleteQueryRequestId
  validateNameAndThrow ResQuery deleteQueryRequestId
  validateQueryAllocation ctx deleteQueryRequestId
  Core.deleteQuery ctx req
  query_stat_add_queries_terminated scStatsHolder "terminated" (-1)
  pure Empty

resumeQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ResumeQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
resumeQueryHandler ctx@ServerContext{scStatsHolder} (ServerNormalRequest _metadata API.ResumeQueryRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Received resume query request. "
           <> "query name: " <> Log.build resumeQueryRequestId
  validateQueryAllocation ctx resumeQueryRequestId
  query_stat_add_queries_terminated scStatsHolder "terminated" (-1)
  query_stat_add_queries_alive scStatsHolder "alive" 1
  Core.resumeQuery ctx resumeQueryRequestId >> returnResp Empty

handleResumeQuery
  :: ServerContext -> G.UnaryHandler API.ResumeQueryRequest Empty
handleResumeQuery ctx@ServerContext{scStatsHolder} _ API.ResumeQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Received resume query request. "
           <> "query name: " <> Log.build resumeQueryRequestId
  validateQueryAllocation ctx resumeQueryRequestId
  Core.resumeQuery ctx resumeQueryRequestId
  query_stat_add_queries_terminated scStatsHolder "terminated" (-1)
  query_stat_add_queries_alive scStatsHolder "alive" 1
  return Empty

-- pauseQueryHandler
--   :: ServerContext
--   -> ServerRequest 'Normal API.PauseQueryRequest Empty
--   -> IO (ServerResponse 'Normal Empty)
-- pauseQueryHandler _ (ServerNormalRequest _metadata req@API.PauseQueryRequest{..}) = queryExceptionHandle $ do
--   Log.debug $ "Received pause query request. "
--            <> "query name: " <> Log.build pauseQueryRequestId
--   validateNameAndThrow pauseQueryRequestId
--   validateQueryAllocation ctx pauseQueryRequestId
--   Core.pauseQuery ctx pauseQueryRequestId >> returnResp Empty

-- handlePauseQuery
--   :: ServerContext -> G.UnaryHandler API.PauseQueryRequest Empty
-- handlePauseQuery _ _ req@API.PauseQueryRequest{..} = catchQueryEx $ do
--   Log.debug $ "Received pause query request. "
--            <> "query name: " <> Log.build pauseQueryRequestId
--   validateNameAndThrow pauseQueryRequestId
--   validateQueryAllocation ctx pauseQueryRequestId
--   Core.pauseQuery ctx pauseQueryRequestId
--   return Empty

validateQueryAllocation :: ServerContext -> T.Text -> IO ()
validateQueryAllocation ctx name = do
  API.ServerNode{..} <- lookupResource' ctx ResQuery name
  unless (serverNodeId == serverID ctx) $
    throwIO $ HE.WrongServer "The Query is allocated to a different node"

--------------------------------------------------------------------------------
-- Exception and Exception Handler

sqlExceptionHandlers :: [HE.Handler (StatusCode, StatusDetails)]
sqlExceptionHandlers = [
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails . BS.pack . formatSomeSQLException $ err))
  ]

sqlExHandlers :: [Handler a]
sqlExHandlers =
  [ Handler $ \(err :: SomeSQLException) -> do
      Log.warning $ Log.buildString' err
      let errmsg = BS.pack . formatSomeSQLException $ err
      G.throwGrpcError $ G.GrpcStatus G.StatusInvalidArgument (Just errmsg) Nothing
  ]

queryExceptionHandle :: HE.ExceptionHandle (ServerResponse 'Normal a)
queryExceptionHandle = HE.mkExceptionHandle . HE.setRespType mkServerErrResp $
  sqlExceptionHandlers ++ defaultHandlers

catchQueryEx :: IO a -> IO a
catchQueryEx action = action `catches` (sqlExHandlers ++ defaultExHandlers)
