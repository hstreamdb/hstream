{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module HStream.Kafka.Server.Handler
  ( handlers
  , unAuthedHandlers
  ) where

import           HStream.Kafka.Server.Handler.AdminCommand
import           HStream.Kafka.Server.Handler.Basic
import           HStream.Kafka.Server.Handler.Consume
import           HStream.Kafka.Server.Handler.Group
import           HStream.Kafka.Server.Handler.Offset
import           HStream.Kafka.Server.Handler.Produce
import           HStream.Kafka.Server.Handler.Security
import           HStream.Kafka.Server.Handler.Topic
import           HStream.Kafka.Server.Types                (ServerContext (..))
import qualified Kafka.Protocol.Message                    as K
import qualified Kafka.Protocol.Service                    as K

-------------------------------------------------------------------------------

#define hsc_lowerfirst(x)                                                      \
  {                                                                            \
    const char* s = (x);                                                       \
    hsc_putchar(hsc_tolower(*s));                                              \
    hsc_printf("%s", ++s);                                                     \
  }

#define hsc_cv_handler(key, start, end, suffix...)                             \
  {                                                                            \
    for (int i = start; i <= end; i++) {                                       \
      hsc_printf("handle%s%sV%d :: ServerContext -> K.RequestContext -> "      \
                 "K.%sRequestV%d -> IO "                                       \
                 "K.%sResponseV%d \n",                                         \
                 #key, #suffix, i, #key, i, #key, i);                          \
      hsc_printf("handle%s%sV%d sc ctx req = K.", #key, #suffix, i);           \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("ResponseToV%d <$> handle%s%s sc ctx (K.", i, #key, #suffix); \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("RequestFromV%d req)\n", i);                                  \
    }                                                                          \
  }

#define hsc_mk_handler(key, start, end, suffix...)                             \
  {                                                                            \
    for (int i = start; i <= end; i++) {                                       \
      if (i != start) {                                                        \
        hsc_printf("  , ");                                                    \
      }                                                                        \
      hsc_printf("K.hd (K.RPC :: K.RPC K.HStreamKafkaV%d \"", i);              \
      hsc_lowerfirst(#key);                                                    \
      hsc_printf("\") (handle%s%sV%d sc)\n", #key, #suffix, i);                \
    }                                                                          \
  }

-------------------------------------------------------------------------------

#cv_handler ApiVersions, 0, 3
#cv_handler ListOffsets, 0, 2
#cv_handler Metadata, 0, 5
#cv_handler Produce, 0, 7
#cv_handler InitProducerId, 0, 0
#cv_handler Fetch, 0, 6
#cv_handler DescribeConfigs, 0, 0

#cv_handler CreateTopics, 0, 2
#cv_handler DeleteTopics, 0, 1
#cv_handler CreatePartitions, 0, 1

#cv_handler FindCoordinator, 0, 1

#cv_handler JoinGroup, 0, 2
#cv_handler SyncGroup, 0, 1
#cv_handler LeaveGroup, 0, 1
#cv_handler Heartbeat, 0, 1
#cv_handler ListGroups, 0, 1
#cv_handler DescribeGroups, 0, 1

#cv_handler OffsetCommit, 0, 3
#cv_handler OffsetFetch, 0, 3

-- Sasl
#cv_handler SaslHandshake, 0, 1
#cv_handler SaslHandshake, 0, 1, AfterAuth
#cv_handler SaslAuthenticate, 0, 0

-- For hstream
#cv_handler HadminCommand, 0, 0

-- ACL
#cv_handler DescribeAcls, 0, 0
#cv_handler CreateAcls, 0, 0
#cv_handler DeleteAcls, 0, 0

handlers :: ServerContext -> [K.ServiceHandler]
handlers sc =
  [ #mk_handler ApiVersions, 0, 3
  , #mk_handler ListOffsets, 0, 2
  , #mk_handler Metadata, 0, 5
    -- Write
  , #mk_handler Produce, 0, 7
  , #mk_handler InitProducerId, 0, 0
    -- Read
  , #mk_handler Fetch, 0, 6

  , #mk_handler FindCoordinator, 0, 1

  , #mk_handler CreateTopics, 0, 2
  , #mk_handler DeleteTopics, 0, 1
  , #mk_handler CreatePartitions, 0, 1

    -- Group
  , #mk_handler JoinGroup, 0, 2
  , #mk_handler SyncGroup, 0, 1
  , #mk_handler LeaveGroup, 0, 1
  , #mk_handler Heartbeat, 0, 1
  , #mk_handler ListGroups, 0, 1
  , #mk_handler DescribeGroups, 0, 1

  , #mk_handler OffsetCommit, 0, 3
  , #mk_handler OffsetFetch, 0, 3

    -- configs
  , #mk_handler DescribeConfigs, 0, 0

    -- Sasl
  , #mk_handler SaslHandshake, 0, 1, AfterAuth
  , #mk_handler SaslAuthenticate, 0, 0

    -- For hstream
  , #mk_handler HadminCommand, 0, 0

    -- ACL
  , #mk_handler DescribeAcls, 0, 0
  , #mk_handler CreateAcls, 0, 0
  , #mk_handler DeleteAcls, 0, 0
  ]

unAuthedHandlers :: ServerContext -> [K.ServiceHandler]
unAuthedHandlers sc =
  [ #mk_handler ApiVersions, 0, 3
  , #mk_handler SaslHandshake, 0, 1
  ]
