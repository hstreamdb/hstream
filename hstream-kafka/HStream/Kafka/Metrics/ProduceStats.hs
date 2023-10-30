module HStream.Kafka.Metrics.ProduceStats where

import qualified Prometheus as P

streamTotalAppendBytes :: P.Vector P.Label2 P.Counter
streamTotalAppendBytes =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "stream_bytes_in" "Successfully appended bytes for a stream"
{-# NOINLINE streamTotalAppendBytes #-}

streamTotalAppendMessages :: P.Vector P.Label2 P.Counter
streamTotalAppendMessages =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "stream_messages_in" "Successfully appended messages for a stream"
{-# NOINLINE streamTotalAppendMessages #-}

appendLatencySnd :: P.Vector P.Label1 P.Histogram
appendLatencySnd =
  P.unsafeRegister . P.vector "streamName" . P.histogram (P.Info "stream_append_latency" "Stream append latency in second") $
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE appendLatencySnd #-}

totalProduceRequest :: P.Vector P.Label2 P.Counter
totalProduceRequest =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "total_produce_request" "Total produce request for a stream"
{-# NOINLINE totalProduceRequest #-}

totalFailedProduceRequest :: P.Vector P.Label2 P.Counter
totalFailedProduceRequest =
  P.unsafeRegister . P.vector ("streamName", "partition") . P.counter $
    P.Info "total_produce_request" "Total failed produce request for a stream"
{-# NOINLINE totalFailedProduceRequest #-}
