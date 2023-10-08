{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Kafka.Server.Handler (handlers) where

import           HStream.Kafka.Server.Handler.Basic
import           HStream.Kafka.Server.Handler.Consume
import           HStream.Kafka.Server.Handler.Offset
import           HStream.Kafka.Server.Handler.Produce
import           HStream.Kafka.Server.Handler.Topic
import           HStream.Kafka.Server.Handler.Group
import           HStream.Kafka.Server.Types           (ServerContext (..))
import qualified Kafka.Protocol.Message               as K
import qualified Kafka.Protocol.Service               as K

handlers :: ServerContext -> [K.ServiceHandler]
handlers sc =
  [ K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "apiVersions") handleApiversionsV0
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "apiVersions") handleApiversionsV1
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV2 "apiVersions") handleApiversionsV2
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV3 "apiVersions") handleApiversionsV3

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "metadata") (handleMetadataV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "metadata") (handleMetadataV1 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "createTopics") (handleCreateTopicsV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "deleteTopics") (handleDeleteTopicsV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV2 "produce") (handleProduceV2 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "fetch") (handleFetchV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "offsetCommit") (handleOffsetCommitV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "offsetFetch") (handleOffsetFetchV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "findCoordinator") (handleFindCoordinatorV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "joinGroup") (handleJoinGroupV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "syncGroup") (handleSyncGroupV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "leaveGroup") (handleLeaveGroupV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "heartbeat") (handleHeartbeatV0 sc)
  ]
