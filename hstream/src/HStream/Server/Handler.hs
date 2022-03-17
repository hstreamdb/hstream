{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler
  (
    handlers,
    -- routineForSubs
  )
where

import           Network.GRPC.HighLevel.Generated

import           HStream.Server.Handler.Admin
import           HStream.Server.Handler.Cluster
import           HStream.Server.Handler.Connector
import           HStream.Server.Handler.Query
import qualified HStream.Server.Handler.Stats        as H
import           HStream.Server.Handler.StoreAdmin
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
  -- timer <- newTimer
  -- _ <- repeatedStart timer (checkSubscriptions timeout serverContext) (msDelay timeout)
  return
    HStreamApi
      { hstreamApiEcho = echoHandler,
        -- Streams
        hstreamApiCreateStream = createStreamHandler serverContext,
        hstreamApiDeleteStream = deleteStreamHandler serverContext,
        hstreamApiListStreams = listStreamsHandler serverContext,
        hstreamApiAppend = appendHandler serverContext,
        hstreamApiAppend0 = append0Handler serverContext,
        -- Subscribe
        hstreamApiCreateSubscription = createSubscriptionHandler serverContext,
        hstreamApiDeleteSubscription = deleteSubscriptionHandler serverContext,
        hstreamApiListSubscriptions = listSubscriptionsHandler serverContext,
        hstreamApiCheckSubscriptionExist = checkSubscriptionExistHandler serverContext,

        hstreamApiStreamingFetch = streamingFetchHandler serverContext,
        -- Stats
        hstreamApiPerStreamTimeSeriesStats = H.perStreamTimeSeriesStats scStatsHolder,
        hstreamApiPerStreamTimeSeriesStatsAll = H.perStreamTimeSeriesStatsAll scStatsHolder,
        -- Query
        hstreamApiTerminateQueries = terminateQueriesHandler serverContext,
        hstreamApiExecuteQuery = executeQueryHandler serverContext,
        hstreamApiExecutePushQuery = executePushQueryHandler serverContext,
        hstreamApiCreateQueryStream = createQueryStreamHandler serverContext,
        -- FIXME:
        hstreamApiCreateQuery = createQueryHandler serverContext,
        hstreamApiGetQuery = getQueryHandler serverContext,
        hstreamApiListQueries = listQueriesHandler serverContext,
        hstreamApiDeleteQuery = deleteQueryHandler serverContext,
        hstreamApiRestartQuery = restartQueryHandler serverContext,
        hstreamApiCreateSinkConnector = createSinkConnectorHandler serverContext,
        hstreamApiGetConnector = getConnectorHandler serverContext,
        hstreamApiListConnectors = listConnectorsHandler serverContext,
        hstreamApiDeleteConnector = deleteConnectorHandler serverContext,
        hstreamApiTerminateConnector = terminateConnectorHandler serverContext,
        hstreamApiRestartConnector = restartConnectorHandler serverContext,
        hstreamApiCreateView = createViewHandler serverContext,
        hstreamApiGetView = getViewHandler serverContext,
        hstreamApiListViews = listViewsHandler serverContext,
        hstreamApiDeleteView = deleteViewHandler serverContext,
        hstreamApiGetNode = getStoreNodeHandler serverContext,
        hstreamApiListNodes = listStoreNodesHandler serverContext,

        -- Cluster
        hstreamApiDescribeCluster = describeClusterHandler serverContext,
        hstreamApiLookupStream = lookupStreamHandler serverContext,
        hstreamApiLookupSubscription = lookupSubscriptionHandler serverContext,
        hstreamApiLookupSubscriptionWithOrderingKey = lookupSubscriptionWithOrderingKeyHandler serverContext
        -- Admin
      , hstreamApiSendAdminCommand = adminCommandHandler serverContext
      }

-------------------------------------------------------------------------------

echoHandler ::
  ServerRequest 'Normal EchoRequest EchoResponse ->
  IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest {..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""
