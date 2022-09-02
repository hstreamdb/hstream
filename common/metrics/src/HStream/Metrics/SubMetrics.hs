{-# LANGUAGE OverloadedStrings #-}

module HStream.Metrics.SubMetrics where

import qualified Prometheus as P

subTotalHandleCountV :: P.Vector P.Label1 P.Counter
subTotalHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "subscription_total_request" "The total number of requests of subscription related handler."
{-# NOINLINE subTotalHandleCountV #-}

subFailedHandleCountV :: P.Vector P.Label1 P.Counter
subFailedHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "subscription_fail_request" "The number of failed requests of subscription related handler."
{-# NOINLINE subFailedHandleCountV #-}

subTotalNumGauge :: P.Vector P.Label1 P.Gauge
subTotalNumGauge =
  P.unsafeRegister . P.vector "streamName" . P.gauge $
      P.Info "total_subscriptions" "Total subscription numbers of a stream."
{-# NOINLINE subTotalNumGauge #-}

subTotalReadBytes :: P.Vector P.Label1 P.Counter
subTotalReadBytes =
  P.unsafeRegister . P.vector "subscriptionId" . P.counter $
      P.Info "total_read_bytes" "Total read bytes from each subscription."
{-# NOINLINE subTotalReadBytes #-}

subTotalReadRecords :: P.Vector P.Label1 P.Counter
subTotalReadRecords =
  P.unsafeRegister . P.vector "subscriptionId" . P.counter $
      P.Info "total_read_records" "Total read records from each subscription."
{-# NOINLINE subTotalReadRecords #-}

lookupSubscriptionLatencySnd :: P.Histogram
lookupSubscriptionLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "lookup_subscription_latency_s" "Lookup subscription latency in second") $
      [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE lookupSubscriptionLatencySnd #-}

readLatencySnd :: P.Histogram
readLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "read_latency_s" "Read from subscription latency in second") $
      [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE readLatencySnd #-}
