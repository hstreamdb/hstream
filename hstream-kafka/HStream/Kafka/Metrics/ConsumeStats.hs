module HStream.Kafka.Metrics.ConsumeStats where

import qualified Prometheus as P

streamTotalSendBytes :: P.Vector P.Label2 P.Counter
streamTotalSendBytes =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "stream_bytes_out" "Successfully read bytes from a stream"
{-# NOINLINE streamTotalSendBytes #-}

streamTotalSendMessages :: P.Vector P.Label2 P.Counter
streamTotalSendMessages =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "stream_messages_out" "Successfully read messages from a stream"
{-# NOINLINE streamTotalSendMessages #-}

readLatencySnd :: P.Histogram
readLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "stream_read_latency" "Stream read latency in second") $
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE readLatencySnd #-}

totalConsumeRequest :: P.Vector P.Label2 P.Counter
totalConsumeRequest =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "total_consume_request" "Total consume request for a stream"
{-# NOINLINE totalConsumeRequest #-}

consumerGroupCommittedOffsets :: P.Vector P.Label3 P.Gauge
consumerGroupCommittedOffsets =
  P.unsafeRegister . P.vector ("group", "streamName", "partition") . P.gauge $
    P.Info "consumer_group_committed_offset" "Latest committed offset for a consumer group"
{-# NOINLINE consumerGroupCommittedOffsets #-}
