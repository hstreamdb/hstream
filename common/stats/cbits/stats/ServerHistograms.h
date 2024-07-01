#pragma once

#include <logdevice/common/stats/Histogram.h>
#include <logdevice/common/stats/HistogramBundle.h>

using facebook::logdevice::HistogramBundle;
using facebook::logdevice::LatencyHistogram;

namespace hstream { namespace common {

struct ServerHistograms : public HistogramBundle {
  HistogramBundle::MapType getMap() override {
    return {
        {"append_request_latency", &append_request_latency},
        {"append_latency", &append_latency},
        {"read_latency", &read_latency},
        {"append_cache_store_latency", &append_cache_store_latency},
        {"read_cache_store_latency", &read_cache_store_latency},
    };
  }

  // Latency of append requests as seen by the server
  LatencyHistogram append_request_latency;
  // Latency of the real writes
  LatencyHistogram append_latency;
  // Latency of logdevice read
  LatencyHistogram read_latency;
  // Latency of cache store writes
  LatencyHistogram append_cache_store_latency;
  // Latency of cache store read
  LatencyHistogram read_cache_store_latency;
};

}} // namespace hstream::common
