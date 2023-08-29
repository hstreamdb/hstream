{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Server.KafkaHandler (handlers) where

import           HStream.Server.KafkaHandler.Basic
import           HStream.Server.Types              (ServerContext (..))
import qualified Kafka.Protocol.Message.Struct     as K
import qualified Kafka.Protocol.Service            as K

handlers :: ServerContext -> [K.ServiceHandler]
handlers sc =
  [ K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "apiVersions") handleApiversionsV0
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "apiVersions") handleApiversionsV1
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV2 "apiVersions") handleApiversionsV2
  ]
