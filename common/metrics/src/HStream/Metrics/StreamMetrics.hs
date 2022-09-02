{-# LANGUAGE OverloadedStrings #-}

module HStream.Metrics.StreamMetrics where

import qualified Prometheus as P

streamTotalHandleCountV :: P.Vector P.Label1 P.Counter
streamTotalHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "stream_total_request" "The total number of requests of stream related handler."
{-# NOINLINE streamTotalHandleCountV #-}

streamFailedHandleCountV :: P.Vector P.Label1 P.Counter
streamFailedHandleCountV =
  P.unsafeRegister . P.vector "handler" . P.counter $
      P.Info "stream_fail_request" "The number of failed requests of stream related handler."
{-# NOINLINE streamFailedHandleCountV #-}

streamTotalNumGauge :: P.Gauge
streamTotalNumGauge =
  P.unsafeRegister . P.gauge $ P.Info "total_streams" "Total stream numbers."
{-# NOINLINE streamTotalNumGauge #-}

streamTotalAppendBytes :: P.Vector P.Label1 P.Counter
streamTotalAppendBytes =
  P.unsafeRegister . P.vector "streamName" . P.counter $
      P.Info "total_append_bytes" "Total append bytes to each stream."
{-# NOINLINE streamTotalAppendBytes #-}

streamTotalAppendRecords :: P.Vector P.Label1 P.Counter
streamTotalAppendRecords =
  P.unsafeRegister . P.vector "streamName" . P.counter $
      P.Info "total_append_records" "Total append records to each stream."
{-# NOINLINE streamTotalAppendRecords #-}

lookupStreamLatencySnd :: P.Histogram
lookupStreamLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "lookup_stream_latency_s" "Lookup stream latency in second") $
      [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE lookupStreamLatencySnd #-}

appendLatencySnd :: P.Histogram
appendLatencySnd =
  P.unsafeRegister . P.histogram (P.Info "append_latency_s" "Append to stream latency in second") $
      [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE appendLatencySnd #-}
