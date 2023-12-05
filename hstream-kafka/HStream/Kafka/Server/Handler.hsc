{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Kafka.Server.Handler
  ( handlers
  , unAuthedHandlers
  ) where

import           HStream.Kafka.Server.Handler.Basic
import           HStream.Kafka.Server.Handler.Consume
import           HStream.Kafka.Server.Handler.Group
import           HStream.Kafka.Server.Handler.Offset
import           HStream.Kafka.Server.Handler.Produce
import           HStream.Kafka.Server.Handler.Security
import           HStream.Kafka.Server.Handler.Topic
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified Kafka.Protocol.Message                as K
import qualified Kafka.Protocol.Service                as K

-------------------------------------------------------------------------------

#define hsc_lowerfirst(x)                                                      \
  {                                                                            \
    const char* s = (x);                                                       \
    hsc_putchar(hsc_tolower(*s));                                              \
    hsc_printf("%s", ++s);                                                     \
  }

#define hsc_cv_handler(key, start, end)                                        \
  {                                                                            \
    for (int i = start; i <= end; i++) {                                       \
      hsc_printf("handle%sV%d :: ServerContext -> K.RequestContext -> "        \
                 "K.%sRequestV%d -> IO "                                       \
                 "K.%sResponseV%d \n",                                         \
                 #key, i, #key, i, #key, i);                                   \
      hsc_printf("handle%sV%d sc ctx req = K.", #key, i);                      \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("ResponseToV%d <$> handle%s sc ctx (K.", i, #key);            \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("RequestFromV%d req)\n", i);                                  \
    }                                                                          \
  }

#define hsc_mk_handler(key, start, end)                                        \
  {                                                                            \
    for (int i = start; i <= end; i++) {                                       \
      if (i != start) {                                                        \
        hsc_printf("  , ");                                                    \
      }                                                                        \
      hsc_printf("K.hd (K.RPC :: K.RPC K.HStreamKafkaV%d \"", i);              \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("\") (handle%sV%d sc)\n", #key, i);                           \
    }                                                                          \
  }

-------------------------------------------------------------------------------

#cv_handler ApiVersions, 0, 3
#cv_handler Produce, 0, 2
#cv_handler Fetch, 0, 3
#cv_handler DescribeConfigs, 0, 0

#cv_handler SaslHandshake, 0, 1
#cv_handler SaslAuthenticate, 0, 0

#cv_handler FindCoordinator, 0, 0

#cv_handler JoinGroup, 0, 2
#cv_handler SyncGroup, 0, 1
#cv_handler LeaveGroup, 0, 1
#cv_handler Heartbeat, 0, 1
#cv_handler ListGroups, 0, 1
#cv_handler DescribeGroups, 0, 1

#cv_handler OffsetCommit, 0, 3
#cv_handler OffsetFetch, 0, 3

handlers :: ServerContext -> [K.ServiceHandler]
handlers sc =
  [ #mk_handler ApiVersions, 0, 3
    -- Write
  , #mk_handler Produce, 0, 2
    -- Read
  , #mk_handler Fetch, 0, 3

  , #mk_handler FindCoordinator, 0, 0

    -- Group
  , #mk_handler JoinGroup, 0, 2
  , #mk_handler SyncGroup, 0, 1
  , #mk_handler LeaveGroup, 0, 1
  , #mk_handler Heartbeat, 0, 1
  , #mk_handler ListGroups, 0, 1
  , #mk_handler DescribeGroups, 0, 1

  , #mk_handler OffsetCommit, 0, 3
  , #mk_handler OffsetFetch, 0, 3

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "metadata") (handleMetadataV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "metadata") (handleMetadataV1 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV2 "metadata") (handleMetadataV2 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV3 "metadata") (handleMetadataV3 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV4 "metadata") (handleMetadataV4 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "createTopics") (handleCreateTopicsV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "deleteTopics") (handleDeleteTopicsV0 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "listOffsets") (handleListOffsetsV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "listOffsets") (handleListOffsetsV1 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "saslHandshake") (handleAfterAuthSaslHandshakeV0 sc)
  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV1 "saslHandshake") (handleAfterAuthSaslHandshakeV1 sc)

  , K.hd (K.RPC :: K.RPC K.HStreamKafkaV0 "saslAuthenticate") (handleAfterAuthSaslAuthenticateV0 sc)

  -- configs
  , #mk_handler DescribeConfigs, 0, 0
  ]

unAuthedHandlers :: ServerContext -> [K.ServiceHandler]
unAuthedHandlers sc =
  [ #mk_handler ApiVersions, 0, 3

  , #mk_handler SaslHandshake, 0, 1
  , #mk_handler SaslAuthenticate, 0, 0
  ]
