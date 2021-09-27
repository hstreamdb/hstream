{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler where

import           Control.Concurrent
import qualified Data.HashMap.Strict                 as HM
import           Data.Int                            (Int64)
import           Network.GRPC.HighLevel.Generated
import           ZooKeeper.Types                     (ZHandle)

import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector
import           HStream.Server.Handler.Query
import qualified HStream.Server.Handler.Stats        as H
import           HStream.Server.Handler.StoreAdmin
import           HStream.Server.Handler.Stream
import           HStream.Server.Handler.Subscription
import           HStream.Server.Handler.View
import qualified HStream.Stats                       as Stats
import qualified HStream.Store                       as S
import qualified HStream.Store.Admin.API             as AA

--------------------------------------------------------------------------------

handlers
  :: S.LDClient
  -> AA.HeaderConfig AA.AdminAPI
  -> Int
  -> ZHandle
  -- ^ timer timeout, ms
  -> Int64
  -> S.Compression
  -> Stats.StatsHolder
  -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient headerConfig repFactor zkHandle _ compression statsHolder = do
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty
  let serverContext =
        ServerContext
          { scLDClient               = ldclient
          , scDefaultStreamRepFactor = repFactor
          , zkHandle                 = zkHandle
          , runningQueries           = runningQs
          , runningConnectors        = runningCs
          , subscribeRuntimeInfo     = subscribeRuntimeInfo
          , cmpStrategy              = compression
          , headerConfig             = headerConfig
          , scStatsHolder            = statsHolder
          }
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
        -- Subscribe
        hstreamApiCreateSubscription = createSubscriptionHandler serverContext,
        hstreamApiDeleteSubscription = deleteSubscriptionHandler serverContext,
        hstreamApiListSubscriptions = listSubscriptionsHandler serverContext,
        hstreamApiCheckSubscriptionExist = checkSubscriptionExistHandler serverContext,
        hstreamApiStreamingFetch = streamingFetchHandler serverContext,
        -- Stats
        hstreamApiPerStreamTimeSeriesStats = H.perStreamTimeSeriesStats statsHolder,
        hstreamApiPerStreamTimeSeriesStatsAll = H.perStreamTimeSeriesStatsAll statsHolder,
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
        hstreamApiListNodes = listStoreNodesHandler serverContext
      }

-------------------------------------------------------------------------------

echoHandler ::
  ServerRequest 'Normal EchoRequest EchoResponse ->
  IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest {..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""
