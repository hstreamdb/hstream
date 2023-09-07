{-# LANGUAGE DataKinds #-}

module HStream.Server.Experimental.StreamV2
  ( doStreamV2Init
  , streamV2Handlers
  ) where

import           Control.Exception                   (catch)
import           Control.Monad
import           HsGrpc.Server
import           Z.Data.CBytes                       (CBytes)
import qualified ZooKeeper                           as ZK
import qualified ZooKeeper.Exception                 as ZK
import qualified ZooKeeper.Types                     as ZK

import qualified HStream.Common.ZookeeperSlotAlloc   as Slot
import qualified HStream.MetaStore.Types             as Meta
import qualified HStream.Server.Handler.Admin        as H
import qualified HStream.Server.Handler.Cluster      as H
import qualified HStream.Server.Handler.Connector    as H
import qualified HStream.Server.Handler.Extra        as H
import qualified HStream.Server.Handler.Query        as H
import qualified HStream.Server.Handler.ShardReader  as H
import qualified HStream.Server.Handler.Stats        as H
import qualified HStream.Server.Handler.Stream       as H
import qualified HStream.Server.Handler.Subscription as H
import qualified HStream.Server.Handler.View         as H
import qualified HStream.Server.HStreamApi           as A
import           HStream.Server.Types                (ServerContext (..))
import qualified HStream.Store                       as S
import qualified HStream.Store.Internal.LogDevice    as LD
import qualified Proto.HStream.Server.HStreamApi     as P

-------------------------------------------------------------------------------

defLogGroupStart :: S.C_LogID
defLogGroupStart = 1000

defLogGroupEnd :: S.C_LogID
defLogGroupEnd = 200000

defLogGroupPath :: CBytes
defLogGroupPath = "/hstream/streamgroup"

defZkRoot :: CBytes
defZkRoot = "/hstream/streamgroup"

doStreamV2Init :: ServerContext -> IO Slot.SlotConfig
doStreamV2Init sc = do
  initDefLogGoup sc
  initZkSlot sc

streamV2Handlers :: ServerContext -> Slot.SlotConfig -> [ServiceHandler]
streamV2Handlers sc slotConfig =
  [ unary (GRPC :: GRPC P.HStreamApi "echo") handleEcho
    -- Cluster
  , unary (GRPC :: GRPC P.HStreamApi "describeCluster") (H.handleDescribeCluster sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupResource") (H.handleLookupResource sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShard") (H.handleLookupShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupSubscription") (H.handleLookupSubscription sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShardReader") (H.handleLookupShardReader sc)
    -- Stream
  , unary (GRPC :: GRPC P.HStreamApi "createStream") (H.handleCreateStreamV2 sc slotConfig)
  , unary (GRPC :: GRPC P.HStreamApi "deleteStream") (H.handleDeleteStreamV2 sc slotConfig)
  --, unary (GRPC :: GRPC P.HStreamApi "getStream") (H.handleGetStream sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listStreams") (H.handleListStreams sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listStreamsWithPrefix") (H.handleListStreamsWithPrefix sc)
  , unary (GRPC :: GRPC P.HStreamApi "listShards") (H.handleListShardV2 sc slotConfig)
  --, unary (GRPC :: GRPC P.HStreamApi "trimStream") (H.handleTrimStream sc)
  --, unary (GRPC :: GRPC P.HStreamApi "trimShard") (H.handleTrimShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "getTailRecordId") (H.handleGetTailRecordIdV2 sc slotConfig)
    -- Reader
  --, unary (GRPC :: GRPC P.HStreamApi "listShardReaders") (H.handleListShardReaders sc)
  --, unary (GRPC :: GRPC P.HStreamApi "createShardReader") (H.handleCreateShardReader sc)
  --, unary (GRPC :: GRPC P.HStreamApi "deleteShardReader") (H.handleDeleteShardReader sc)
    -- Subscription
  --, unary (GRPC :: GRPC P.HStreamApi "createSubscription") (H.handleCreateSubscription sc)
  --, unary (GRPC :: GRPC P.HStreamApi "getSubscription") (H.handleGetSubscription sc)
  --, handlerUseThreadPool $ unary (GRPC :: GRPC P.HStreamApi "deleteSubscription") (H.handleDeleteSubscription sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listSubscriptions") (H.handleListSubscriptions sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listSubscriptionsWithPrefix") (H.handleListSubscriptionsWithPrefix sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listConsumers") (H.handleListConsumers sc)
  --, unary (GRPC :: GRPC P.HStreamApi "checkSubscriptionExist") (H.handleCheckSubscriptionExist sc)
    -- Append
  , unary (GRPC :: GRPC P.HStreamApi "append") (H.handleAppend sc)
    -- Read
  , unary (GRPC :: GRPC P.HStreamApi "readShard") (H.handleReadShard sc)
  , serverStream (GRPC :: GRPC P.HStreamApi "readShardStream") (H.handleReadShardStream sc)
  , serverStream (GRPC :: GRPC P.HStreamApi "readSingleShardStream") (H.handleReadSingleShardStream sc)
  --, serverStream (GRPC :: GRPC P.HStreamApi "readStream") (H.handleReadStream sc)
    -- Subscribe
  --, bidiStream (GRPC :: GRPC P.HStreamApi "streamingFetch") (H.handleStreamingFetch sc)
    -- Stats
  , unary (GRPC :: GRPC P.HStreamApi "perStreamTimeSeriesStats") (H.handlePerStreamTimeSeriesStats $ scStatsHolder sc)
  , unary (GRPC :: GRPC P.HStreamApi "perStreamTimeSeriesStatsAll") (H.handlePerStreamTimeSeriesStatsAll $ scStatsHolder sc)
  , unary (GRPC :: GRPC P.HStreamApi "getStats") (H.handleGetStats $ sc)
    -- Admin
  , unary (GRPC :: GRPC P.HStreamApi "sendAdminCommand") (H.handleAdminCommand sc)
    -- Connector
  --, unary (GRPC :: GRPC P.HStreamApi "createConnector") (H.handleCreateConnector sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listConnectors") (H.handleListConnectors sc)
  --, unary (GRPC :: GRPC P.HStreamApi "getConnector") (H.handleGetConnector sc)
  --, unary (GRPC :: GRPC P.HStreamApi "getConnectorSpec") (H.handleGetConnectorSpec sc)
  --, unary (GRPC :: GRPC P.HStreamApi "getConnectorLogs") (H.handleGetConnectorLogs sc)
  --, unary (GRPC :: GRPC P.HStreamApi "deleteConnector") (H.handleDeleteConnector sc)
  --, unary (GRPC :: GRPC P.HStreamApi "resumeConnector") (H.handleResumeConnector sc)
  --, unary (GRPC :: GRPC P.HStreamApi "pauseConnector") (H.handlePauseConnector sc)
    -- View
  --, unary (GRPC :: GRPC P.HStreamApi "getView") (H.handleGetView sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listViews") (H.handleListView sc)
  --, unary (GRPC :: GRPC P.HStreamApi "deleteView") (H.handleDeleteView sc)
  --, unary (GRPC :: GRPC P.HStreamApi "executeViewQuery") (H.handleExecuteViewQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "executeViewQueryWithNamespace") (H.handleExecuteViewQueryWithNamespace sc)
    -- Query
  --, unary (GRPC :: GRPC P.HStreamApi "terminateQuery") (H.handleTerminateQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "executeQuery") (H.handleExecuteQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "getQuery") (H.handleGetQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "createQuery") (H.handleCreateQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "createQueryWithNamespace") (H.handleCreateQueryWithNamespace sc)
  --, unary (GRPC :: GRPC P.HStreamApi "listQueries") (H.handleListQueries sc)
  --, unary (GRPC :: GRPC P.HStreamApi "deleteQuery") (H.handleDeleteQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "resumeQuery") (H.handleResumeQuery sc)
  --, unary (GRPC :: GRPC P.HStreamApi "parseSql") (H.handleParseSql sc)
  ]

-------------------------------------------------------------------------------

initDefLogGoup :: ServerContext -> IO ()
initDefLogGoup ServerContext{..} = do
  let attrs = S.def{ S.logReplicationFactor = S.defAttr1 1
                   , S.logBacklogDuration   = S.defAttr1 (Just 600)
                   }
  catch (do group <- LD.makeLogGroup scLDClient defLogGroupPath defLogGroupStart defLogGroupEnd attrs True
            LD.syncLogsConfigVersion scLDClient =<< LD.logGroupGetVersion group
        ) $ \(_ :: S.EXISTS) -> pure ()

initZkSlot :: ServerContext -> IO Slot.SlotConfig
initZkSlot sc@ServerContext{..} = do
  case metaHandle of
    Meta.ZkHandle zh -> do
      catch (void $ ZK.zooCreate zh "/hstream" Nothing ZK.zooOpenAclUnsafe ZK.ZooPersistent) $
        \(_ :: ZK.ZNODEEXISTS) -> pure ()
      let slotConfig = Slot.SlotConfig{ slotRoot = defZkRoot
                                      , slotZkHandler = zh
                                      , slotOffset = defLogGroupStart
                                      , slotMaxCapbility = defLogGroupEnd - defLogGroupStart + 1
                                      }
      Slot.initSlot slotConfig
      pure slotConfig
    _ -> errorWithoutStackTrace "This stream-v2 feature should be used with zookeeper meta store!"

handleEcho :: UnaryHandler A.EchoRequest A.EchoResponse
handleEcho _grpcCtx A.EchoRequest{..} = return $ A.EchoResponse echoRequestMsg
