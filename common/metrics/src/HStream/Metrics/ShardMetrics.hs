{-# LANGUAGE OverloadedStrings #-}

module HStream.Metrics.ShardMetrics where

import qualified Prometheus as P

shardTotalHandleCountV :: P.Vector P.Label1 P.Counter
shardTotalHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "shard_total_request" "The total number of requests of shard related handler."
{-# NOINLINE shardTotalHandleCountV #-}

shardFailedHandleCountV :: P.Vector P.Label1 P.Counter
shardFailedHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "shard_fail_request" "The number of failed request of shard related handler."
{-# NOINLINE shardFailedHandleCountV #-}

shardsTotalNumGauge :: P.Vector P.Label1 P.Gauge
shardsTotalNumGauge =
  P.unsafeRegister . P.vector "streamName" . P.gauge $ P.Info "total_shards" "Total shards numbers."
{-# NOINLINE shardsTotalNumGauge #-}
