module HStream.Kafka.Metrics.ServerStats where

import qualified Prometheus as P

totalRequests :: P.Counter
totalRequests =
  P.unsafeRegister . P.counter $ P.Info "total_requests" "Total number of requests received by server"
{-# NOINLINE totalRequests #-}

handlerLatencies :: P.Vector P.Label1 P.Histogram
handlerLatencies =
  P.unsafeRegister . P.vector "handler" $
      P.histogram (P.Info "handler_latency" "Total processing time for a handler in second")
        [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
{-# NOINLINE handlerLatencies #-}
