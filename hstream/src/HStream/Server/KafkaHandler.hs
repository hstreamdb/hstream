{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Server.KafkaHandler (handlers) where

import           HStream.Server.KafkaHandler.Basic
import           HStream.Server.KafkaHandler.Consume
import           HStream.Server.KafkaHandler.Offset
import           HStream.Server.KafkaHandler.Produce
import           HStream.Server.KafkaHandler.Topic
import           HStream.Server.Types                (ServerContext (..))
import qualified Kafka.Protocol.Message              as K
import qualified Kafka.Protocol.Service              as K

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
  ]
