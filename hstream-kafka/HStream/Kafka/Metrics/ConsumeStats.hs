module HStream.Kafka.Metrics.ConsumeStats where

import qualified Prometheus as P

topicTotalSendBytes :: P.Vector P.Label2 P.Counter
topicTotalSendBytes =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "topic_bytes_out" "Successfully read bytes from a topic"
{-# NOINLINE topicTotalSendBytes #-}

topicTotalSendMessages :: P.Vector P.Label2 P.Counter
topicTotalSendMessages =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "topic_messages_out" "Successfully read messages from a topic"
{-# NOINLINE topicTotalSendMessages #-}

readLatencySnd :: P.Histogram
readLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "topic_read_latency" "topic read latency in second") $
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE readLatencySnd #-}

totalConsumeRequest :: P.Vector P.Label2 P.Counter
totalConsumeRequest =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "total_consume_request" "Total consume request for a topic"
{-# NOINLINE totalConsumeRequest #-}

consumerGroupCommittedOffsets :: P.Vector P.Label3 P.Gauge
consumerGroupCommittedOffsets =
  P.unsafeRegister . P.vector ("group", "topicName", "partition") . P.gauge $
    P.Info "consumer_group_committed_offset" "Latest committed offset for a consumer group"
{-# NOINLINE consumerGroupCommittedOffsets #-}
