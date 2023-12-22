module HStream.Kafka.Common.Metrics
  ( startMetricsServer
  , observeWithLabel
  , P.withLabel
  , P.setGauge
  , P.addCounter
  , P.incCounter
  , P.observeDuration

  , module HStream.Kafka.Common.Metrics.ConsumeStats
  , module HStream.Kafka.Common.Metrics.ProduceStats
  , module HStream.Kafka.Common.Metrics.ServerStats
  ) where

import           Data.Ratio                                ((%))
import           Data.String                               (fromString)
import qualified Network.Wai.Handler.Warp                  as Warp
import qualified Network.Wai.Middleware.Prometheus         as P
import qualified Prometheus                                as P
import           System.Clock                              (Clock (..),
                                                            diffTimeSpec,
                                                            getTime, toNanoSecs)

import           HStream.Kafka.Common.Metrics.ConsumeStats
import           HStream.Kafka.Common.Metrics.ProduceStats
import           HStream.Kafka.Common.Metrics.ServerStats

-- | Start a prometheus server
--
-- Note: The host recognizes the following special values:
--
--   * means HostAny - "any IPv4 or IPv6 hostname"
--   *4 means HostIPv4 - "any IPv4 or IPv6 hostname, IPv4 preferred"
--   !4 means HostIPv4Only - "any IPv4 hostname"
--   *6 means HostIPv6@ - "any IPv4 or IPv6 hostname, IPv6 preferred"
--   !6 means HostIPv6Only - "any IPv6 hostname"
--
-- Note that the permissive * values allow binding to an IPv4 or an IPv6
-- hostname, which means you might be able to successfully bind to a port more
-- times than you expect (eg once on the IPv4 localhost 127.0.0.1 and again on
-- the IPv6 localhost 0:0:0:0:0:0:0:1).
--
-- Any other value is treated as a hostname. As an example, to bind to the IPv4
-- local host only, use "127.0.0.1".
startMetricsServer :: String -> Int -> IO ()
startMetricsServer host port = do
  let settings = Warp.setHost (fromString host)
               $ Warp.setPort port
               $ Warp.defaultSettings
  Warp.runSettings settings $
    P.prometheus P.def{P.prometheusInstrumentPrometheus = False} P.metricsApp

observeWithLabel
  :: (P.Observer metric, P.Label label)
  => P.Vector label metric
  -> label
  -> IO a
  -> IO a
observeWithLabel metric labels action = do
  start <- getTime Monotonic
  result <- action
  end <- getTime Monotonic
  let duration = toNanoSecs (end `diffTimeSpec` start) % 1000000000
  P.withLabel metric labels $ flip P.observe (fromRational duration)
  return result
