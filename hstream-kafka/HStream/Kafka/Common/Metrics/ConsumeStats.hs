module HStream.Kafka.Common.Metrics.ConsumeStats where

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

topicReadStoreLatency :: P.Histogram
topicReadStoreLatency =
  P.unsafeRegister . P.histogram (P.Info "topic_read_store_latency" "topic read store latency in second") $
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE topicReadStoreLatency #-}

totalConsumeRequest :: P.Vector P.Label2 P.Counter
totalConsumeRequest =
  P.unsafeRegister . P.vector ("topicName", "partition") . P.counter $
    P.Info "total_consume_request" "Total consume request for a topic"
{-# NOINLINE totalConsumeRequest #-}

totalOffsetCommitRequest :: P.Vector P.Label1 P.Counter
totalOffsetCommitRequest =
  P.unsafeRegister . P.vector "consumer_group" . P.counter $
    P.Info "total_offset_commit_request" "Total offset commit request for a consumer group"
{-# NOINLINE totalOffsetCommitRequest #-}

totalFailedOffsetCommitRequest :: P.Vector P.Label1 P.Counter
totalFailedOffsetCommitRequest =
  P.unsafeRegister . P.vector "consumer_group" . P.counter $
    P.Info "total_failed_offset_commit_request" "Total failed offset commit request for a consumer group"
{-# NOINLINE totalFailedOffsetCommitRequest #-}

consumerGroupCommittedOffsets :: P.Vector P.Label3 P.Gauge
consumerGroupCommittedOffsets =
  P.unsafeRegister . P.vector ("consumer_group", "topicName", "partition") . P.gauge $
    P.Info "consumer_group_committed_offset" "Latest committed offset for a consumer group"
{-# NOINLINE consumerGroupCommittedOffsets #-}
