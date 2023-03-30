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
import           HStream.Server.Handler.Query
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
        -- Reader
        hstreamApiListShardReaders = listShardReadersHandler serverContext,
        hstreamApiCreateShardReader = createShardReaderHandler serverContext,
        hstreamApiDeleteShardReader = deleteShardReaderHandler serverContext,
        hstreamApiReadShard         = readShardHandler serverContext,

        -- Stats
        hstreamApiPerStreamTimeSeriesStats = H.perStreamTimeSeriesStats scStatsHolder,
        hstreamApiPerStreamTimeSeriesStatsAll = H.perStreamTimeSeriesStatsAll scStatsHolder,
        hstreamApiGetStats = H.getStatsHandler scStatsHolder,

        -- Query
        hstreamApiTerminateQueries = terminateQueriesHandler serverContext,
        hstreamApiExecuteQuery = executeQueryHandler serverContext,
        -- FIXME:
        hstreamApiGetQuery = getQueryHandler serverContext,
        hstreamApiCreateQuery = createQueryHandler serverContext,
        hstreamApiCreateQueryWithNamespace = createQueryWithNamespaceHandler serverContext,
        hstreamApiListQueries = listQueriesHandler serverContext,
        hstreamApiDeleteQuery = deleteQueryHandler serverContext,
        hstreamApiRestartQuery = restartQueryHandler serverContext,

        -- Connector
        hstreamApiCreateConnector = createConnectorHandler serverContext,
        hstreamApiGetConnector = getConnectorHandler serverContext,
        hstreamApiGetConnectorSpec = getConnectorSpecHandler serverContext,
        hstreamApiListConnectors = listConnectorsHandler serverContext,
        hstreamApiDeleteConnector = deleteConnectorHandler serverContext,
        hstreamApiPauseConnector = pauseConnectorHandler serverContext,
        hstreamApiResumeConnector = resumeConnectorHandler serverContext,

        -- View
        hstreamApiGetView = getViewHandler serverContext,
        hstreamApiListViews = listViewsHandler serverContext,
        hstreamApiDeleteView = deleteViewHandler serverContext,

        -- Cluster
        hstreamApiDescribeCluster    = describeClusterHandler serverContext,
        hstreamApiLookupResource     = lookupResourceHandler serverContext,
        hstreamApiLookupShard        = lookupShardHandler serverContext,
        hstreamApiLookupSubscription = lookupSubscriptionHandler serverContext,
        hstreamApiLookupShardReader  = lookupShardReaderHandler serverContext,

        -- Admin
        hstreamApiSendAdminCommand = adminCommandHandler serverContext
      }

-------------------------------------------------------------------------------

echoHandler ::
  ServerRequest 'Normal EchoRequest EchoResponse ->
  IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest {..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""
