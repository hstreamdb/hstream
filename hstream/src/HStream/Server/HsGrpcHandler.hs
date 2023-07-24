{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Server.HsGrpcHandler (handlers) where

import           HsGrpc.Server

import qualified HStream.Server.Handler.Admin        as H
import qualified HStream.Server.Handler.Cluster      as H
import qualified HStream.Server.Handler.Connector    as H
import qualified HStream.Server.Handler.Extra        as H
import qualified HStream.Server.Handler.Query        as H
import qualified HStream.Server.Handler.Schema       as H
import qualified HStream.Server.Handler.ShardReader  as H
import qualified HStream.Server.Handler.Stats        as H
import qualified HStream.Server.Handler.Stream       as H
import qualified HStream.Server.Handler.Subscription as H
import qualified HStream.Server.Handler.View         as H
import qualified HStream.Server.HStreamApi           as A
import           HStream.Server.Types                (ServerContext (..))
import qualified Proto.HStream.Server.HStreamApi     as P

-------------------------------------------------------------------------------

handlers :: ServerContext -> [ServiceHandler]
handlers sc =
  [ unary (GRPC :: GRPC P.HStreamApi "echo") handleEcho
    -- Cluster
  , unary (GRPC :: GRPC P.HStreamApi "describeCluster") (H.handleDescribeCluster sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupResource") (H.handleLookupResource sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShard") (H.handleLookupShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupSubscription") (H.handleLookupSubscription sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShardReader") (H.handleLookupShardReader sc)
    -- Stream
  , unary (GRPC :: GRPC P.HStreamApi "createStream") (H.handleCreateStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "deleteStream") (H.handleDeleteStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "getStream") (H.handleGetStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "listStreams") (H.handleListStreams sc)
  , unary (GRPC :: GRPC P.HStreamApi "listStreamsWithPrefix") (H.handleListStreamsWithPrefix sc)
  , unary (GRPC :: GRPC P.HStreamApi "trimStream") (H.handleTrimStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "listShards") (H.handleListShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "trimShard") (H.handleTrimShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "getTailRecordId") (H.handleGetTailRecordId sc)
    -- Reader
  , unary (GRPC :: GRPC P.HStreamApi "listShardReaders") (H.handleListShardReaders sc)
  , unary (GRPC :: GRPC P.HStreamApi "createShardReader") (H.handleCreateShardReader sc)
  , unary (GRPC :: GRPC P.HStreamApi "deleteShardReader") (H.handleDeleteShardReader sc)
    -- Subscription
  , unary (GRPC :: GRPC P.HStreamApi "createSubscription") (H.handleCreateSubscription sc)
  , unary (GRPC :: GRPC P.HStreamApi "getSubscription") (H.handleGetSubscription sc)
  , handlerUseThreadPool $ unary (GRPC :: GRPC P.HStreamApi "deleteSubscription") (H.handleDeleteSubscription sc)
  , unary (GRPC :: GRPC P.HStreamApi "listSubscriptions") (H.handleListSubscriptions sc)
  , unary (GRPC :: GRPC P.HStreamApi "listSubscriptionsWithPrefix") (H.handleListSubscriptionsWithPrefix sc)
  , unary (GRPC :: GRPC P.HStreamApi "listConsumers") (H.handleListConsumers sc)
  , unary (GRPC :: GRPC P.HStreamApi "checkSubscriptionExist") (H.handleCheckSubscriptionExist sc)
    -- Append
  , unary (GRPC :: GRPC P.HStreamApi "append") (H.handleAppend sc)
    -- Read
  , unary (GRPC :: GRPC P.HStreamApi "readShard") (H.handleReadShard sc)
  , serverStream (GRPC :: GRPC P.HStreamApi "readShardStream") (H.handleReadShardStream sc)
  , serverStream (GRPC :: GRPC P.HStreamApi "readStream") (H.handleReadStream sc)
  , serverStream (GRPC :: GRPC P.HStreamApi "readSingleShardStream") (H.handleReadSingleShardStream sc)
  , bidiStream (GRPC :: GRPC P.HStreamApi "readStreamByKey") (H.handleReadStreamByKey sc)
    -- Subscribe
  , bidiStream (GRPC :: GRPC P.HStreamApi "streamingFetch") (H.handleStreamingFetch sc)
    -- Stats
  , unary (GRPC :: GRPC P.HStreamApi "perStreamTimeSeriesStats") (H.handlePerStreamTimeSeriesStats $ scStatsHolder sc)
  , unary (GRPC :: GRPC P.HStreamApi "perStreamTimeSeriesStatsAll") (H.handlePerStreamTimeSeriesStatsAll $ scStatsHolder sc)
  , unary (GRPC :: GRPC P.HStreamApi "getStats") (H.handleGetStats $ scStatsHolder sc)
    -- Admin
  , unary (GRPC :: GRPC P.HStreamApi "sendAdminCommand") (H.handleAdminCommand sc)
    -- Connector
  , unary (GRPC :: GRPC P.HStreamApi "createConnector") (H.handleCreateConnector sc)
  , unary (GRPC :: GRPC P.HStreamApi "listConnectors") (H.handleListConnectors sc)
  , unary (GRPC :: GRPC P.HStreamApi "getConnector") (H.handleGetConnector sc)
  , unary (GRPC :: GRPC P.HStreamApi "getConnectorSpec") (H.handleGetConnectorSpec sc)
  , unary (GRPC :: GRPC P.HStreamApi "getConnectorLogs") (H.handleGetConnectorLogs sc)
  , unary (GRPC :: GRPC P.HStreamApi "deleteConnector") (H.handleDeleteConnector sc)
  , unary (GRPC :: GRPC P.HStreamApi "resumeConnector") (H.handleResumeConnector sc)
  , unary (GRPC :: GRPC P.HStreamApi "pauseConnector") (H.handlePauseConnector sc)
    -- View
  , unary (GRPC :: GRPC P.HStreamApi "getView") (H.handleGetView sc)
  , unary (GRPC :: GRPC P.HStreamApi "listViews") (H.handleListView sc)
  , unary (GRPC :: GRPC P.HStreamApi "deleteView") (H.handleDeleteView sc)
  , unary (GRPC :: GRPC P.HStreamApi "executeViewQuery") (H.handleExecuteViewQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "executeViewQueryWithNamespace") (H.handleExecuteViewQueryWithNamespace sc)
    -- Query
  , unary (GRPC :: GRPC P.HStreamApi "terminateQuery") (H.handleTerminateQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "executeQuery") (H.handleExecuteQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "getQuery") (H.handleGetQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "createQuery") (H.handleCreateQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "createQueryWithNamespace") (H.handleCreateQueryWithNamespace sc)
  , unary (GRPC :: GRPC P.HStreamApi "listQueries") (H.handleListQueries sc)
  , unary (GRPC :: GRPC P.HStreamApi "deleteQuery") (H.handleDeleteQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "resumeQuery") (H.handleResumeQuery sc)
  , unary (GRPC :: GRPC P.HStreamApi "parseSql") (H.handleParseSql sc)

#ifdef HStreamEnableSchema
  , unary (GRPC :: GRPC P.HStreamApi "registerSchema") (H.handleRegisterSchema sc)
  , unary (GRPC :: GRPC P.HStreamApi "getSchema") (H.handleGetSchema sc)
  , unary (GRPC :: GRPC P.HStreamApi "unregisterSchema") (H.handleUnregisterSchema sc)
#endif
  ]

handleEcho :: UnaryHandler A.EchoRequest A.EchoResponse
handleEcho _grpcCtx A.EchoRequest{..} = return $ A.EchoResponse echoRequestMsg
