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
    };
  }

  // Latency of append requests as seen by the server
  LatencyHistogram append_request_latency;
  // Latency of the real writes
  LatencyHistogram append_latency;
};

}} // namespace hstream::common
