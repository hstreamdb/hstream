module HStream.Kafka.Common.Metrics.ProduceStats where

import qualified Prometheus as P

topicTotalAppendBytes :: P.Vector P.Label2 P.Counter
topicTotalAppendBytes =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "topic_bytes_in" "Successfully appended bytes for a topic"
{-# NOINLINE topicTotalAppendBytes #-}

topicTotalAppendMessages :: P.Vector P.Label2 P.Counter
topicTotalAppendMessages =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "topic_messages_in" "Successfully appended messages for a topic"
{-# NOINLINE topicTotalAppendMessages #-}

appendLatencySnd :: P.Vector P.Label1 P.Histogram
appendLatencySnd =
  P.unsafeRegister . P.vector "topicName" . P.histogram (P.Info "topic_append_latency" "topic append latency in second") $
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE appendLatencySnd #-}

totalProduceRequest :: P.Vector P.Label2 P.Counter
totalProduceRequest =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "total_produce_request" "Total produce request for a topic"
{-# NOINLINE totalProduceRequest #-}

totalFailedProduceRequest :: P.Vector P.Label2 P.Counter
totalFailedProduceRequest =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "total_failed_produce_request" "Total failed produce request for a topic"
{-# NOINLINE totalFailedProduceRequest #-}
