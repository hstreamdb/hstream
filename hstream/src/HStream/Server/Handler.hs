{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler
  ( handlers
  )
where

import           Network.GRPC.HighLevel.Generated

import           HStream.Server.Handler.Admin
import           HStream.Server.Handler.Cluster
import           HStream.Server.Handler.Connector
import           HStream.Server.Handler.Extra
import           HStream.Server.Handler.Query
import           HStream.Server.Handler.Schema
import           HStream.Server.Handler.ShardReader
import qualified HStream.Server.Handler.Stats        as H
import           HStream.Server.Handler.Stream
import           HStream.Server.Handler.Subscription
import           HStream.Server.Handler.View
import           HStream.Server.HStreamApi
import           HStream.Server.Types

--------------------------------------------------------------------------------

handlers
  :: ServerContext
  -> IO (HStreamApi ServerRequest ServerResponse)
handlers serverContext@ServerContext{..} =
  return
    HStreamApi
      { hstreamApiEcho = echoHandler,
        -- Streams
        hstreamApiCreateStream = createStreamHandler serverContext,
        hstreamApiDeleteStream = deleteStreamHandler serverContext,
        hstreamApiGetStream = getStreamHandler serverContext,
        hstreamApiListStreams = listStreamsHandler serverContext,
        hstreamApiListStreamsWithPrefix = listStreamsWithPrefixHandler serverContext,
        hstreamApiAppend = appendHandler serverContext,
        hstreamApiGetTailRecordId = getTailRecordIdHandler serverContext,
        hstreamApiTrimStream  = trimStreamHandler serverContext,
        hstreamApiTrimBeforeRecordIds  = trimBeforeRecordIdsHandler serverContext,

        -- Subscribe
        hstreamApiCreateSubscription = createSubscriptionHandler serverContext,
        hstreamApiDeleteSubscription = deleteSubscriptionHandler serverContext,
        hstreamApiGetSubscription = getSubscriptionHandler serverContext,
        hstreamApiListSubscriptions = listSubscriptionsHandler serverContext,
        hstreamApiListSubscriptionsWithPrefix = listSubscriptionsWithPrefixHandler serverContext,
        hstreamApiListConsumers = listConsumersHandler serverContext,
        hstreamApiCheckSubscriptionExist = checkSubscriptionExistHandler serverContext,

        hstreamApiStreamingFetch = streamingFetchHandler serverContext,

        -- Shards
        hstreamApiListShards = listShardsHandler serverContext,
        hstreamApiTrimShard  = trimShardHandler serverContext,
        -- Reader
        hstreamApiListShardReaders  = listShardReadersHandler serverContext,
        hstreamApiCreateShardReader = createShardReaderHandler serverContext,
        hstreamApiDeleteShardReader = deleteShardReaderHandler serverContext,
        hstreamApiReadShard         = readShardHandler serverContext,
        hstreamApiReadShardStream   = readShardStreamHandler serverContext,
        hstreamApiReadStream        = readStreamHandler serverContext,
        hstreamApiReadSingleShardStream = readSingleShardStreamHandler serverContext,
        hstreamApiReadStreamByKey = readStreamByKeyHandler serverContext,

        -- Stats
        hstreamApiPerStreamTimeSeriesStats = H.perStreamTimeSeriesStats scStatsHolder,
        hstreamApiPerStreamTimeSeriesStatsAll = H.perStreamTimeSeriesStatsAll scStatsHolder,
        hstreamApiGetStats = H.getStatsHandler scStatsHolder,

        -- Query
        hstreamApiTerminateQuery = terminateQueryHandler serverContext,
        hstreamApiExecuteQuery = executeQueryHandler serverContext,
        -- FIXME:
        hstreamApiGetQuery = getQueryHandler serverContext,
        hstreamApiCreateQuery = createQueryHandler serverContext,
        hstreamApiCreateQueryWithNamespace = createQueryWithNamespaceHandler serverContext,
        hstreamApiListQueries = listQueriesHandler serverContext,
        hstreamApiDeleteQuery = deleteQueryHandler serverContext,
        hstreamApiResumeQuery = resumeQueryHandler serverContext,

#ifdef HStreamEnableSchema
        hstreamApiRegisterSchema = registerSchemaHandler serverContext,
        hstreamApiGetSchema = getSchemaHandler serverContext,
        hstreamApiUnregisterSchema = unregisterSchemaHandler serverContext,
#endif

        -- Connector
        hstreamApiCreateConnector = createConnectorHandler serverContext,
        hstreamApiGetConnector = getConnectorHandler serverContext,
        hstreamApiGetConnectorSpec = getConnectorSpecHandler serverContext,
        hstreamApiGetConnectorLogs = getConnectorLogsHandler serverContext,
        hstreamApiListConnectors = listConnectorsHandler serverContext,
        hstreamApiDeleteConnector = deleteConnectorHandler serverContext,
        hstreamApiPauseConnector = pauseConnectorHandler serverContext,
        hstreamApiResumeConnector = resumeConnectorHandler serverContext,

        -- View
        hstreamApiGetView = getViewHandler serverContext,
        hstreamApiListViews = listViewsHandler serverContext,
        hstreamApiDeleteView = deleteViewHandler serverContext,
        hstreamApiExecuteViewQuery = executeViewQueryHandler serverContext,
        hstreamApiExecuteViewQueryWithNamespace = executeViewQueryWithNamespaceHandler serverContext,

        -- Cluster
        hstreamApiDescribeCluster    = describeClusterHandler serverContext,
        hstreamApiLookupResource     = lookupResourceHandler serverContext,
        hstreamApiLookupShard        = lookupShardHandler serverContext,
        hstreamApiLookupSubscription = lookupSubscriptionHandler serverContext,
        hstreamApiLookupShardReader  = lookupShardReaderHandler serverContext,
        hstreamApiLookupKey          = lookupKeyHandler serverContext,

        -- Admin
        hstreamApiSendAdminCommand = adminCommandHandler serverContext,

        hstreamApiParseSql = parseSqlHandler serverContext
      }

-------------------------------------------------------------------------------

echoHandler ::
  ServerRequest 'Normal EchoRequest EchoResponse ->
  IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest {..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""
