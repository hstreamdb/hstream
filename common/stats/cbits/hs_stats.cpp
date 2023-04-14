#include "hs_stats.h"

#include <boost/format.hpp>
#include <logdevice/common/commandline_util_chrono.h>

using facebook::logdevice::chrono_string;

extern "C" {
// ----------------------------------------------------------------------------

void setPerStreamTimeSeriesMember(
    const char* stat_name,
    std::shared_ptr<PerStreamTimeSeries> PerStreamStats::*& member_ptr) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == std::string(stat_name)) {                                       \
      member_ptr = &PerStreamStats::name;                                      \
      break;                                                                   \
    }                                                                          \
  }
#include "per_stream_time_series.inc"
}

void setPerStreamStatsMember(const char* stat_name,
                             StatsCounter PerStreamStats::*& member_ptr) {
#define STAT_DEFINE(name, _)                                                   \
  if (#name == std::string(stat_name)) {                                       \
    member_ptr = &PerStreamStats::name##_counter;                              \
  }
#include "per_stream_stats.inc"
}

void setPerConnectorStatsMember(const char* stat_name,
                                StatsCounter PerConnectorStats::*& member_ptr) {
#define STAT_DEFINE(name, _)                                                   \
  if (#name == std::string(stat_name)) {                                       \
    member_ptr = &PerConnectorStats::name##_counter;                           \
  }
#include "per_connector_stats.inc"
}


void setPerSubscriptionTimeSeriesMember(
    const char* stat_name, std::shared_ptr<PerSubscriptionTimeSeries>
                               PerSubscriptionStats::*& member_ptr) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == std::string(stat_name)) {                                       \
      member_ptr = &PerSubscriptionStats::name;                                \
      break;                                                                   \
    }                                                                          \
  }
#include "per_subscription_time_series.inc"
}

void setPerSubscriptionStatsMember(
    const char* stat_name, StatsCounter PerSubscriptionStats::*& member_ptr) {
#define STAT_DEFINE(name, _)                                                   \
  if (#name == std::string(stat_name)) {                                       \
    member_ptr = &PerSubscriptionStats::name##_counter;                        \
  }
#include "per_subscription_stats.inc"
}

void setPerHandleTimeSeriesMember(
    const char* stat_name,
    std::shared_ptr<PerHandleTimeSeries> PerHandleStats::*& member_ptr) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == std::string(stat_name)) {                                       \
      member_ptr = &PerHandleStats::name;                                      \
      break;                                                                   \
    }                                                                          \
  }
#include "per_handle_time_series.inc"
}



// ----------------------------------------------------------------------------

StatsHolder* new_stats_holder(HsBool is_server) {
  return new StatsHolder(StatsParams().setIsServer(is_server));
}

void delete_stats_holder(StatsHolder* s) { delete s; }

// TODO: split into a specific aggregate function. e.g.
// new_aggregate_stream_stats
Stats* new_aggregate_stats(StatsHolder* s) { return s->aggregate(); }
void delete_stats(Stats* s) { delete s; }

void stats_holder_print(StatsHolder* s) { s->print(); }

void stats_holder_reset(StatsHolder* s) { s->reset(); }

// ----------------------------------------------------------------------------
// PerStreamStats

#define STAT_DEFINE(name, _)                                                   \
  PER_X_STAT_DEFINE(stream_stat_, per_stream_stats, PerStreamStats, name)
#include "per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  PER_X_TIME_SERIES_DEFINE(stream_time_series_, per_stream_stats,              \
                           PerStreamStats, PerStreamTimeSeries, name)
#include "per_stream_time_series.inc"

int stream_stat_getall(StatsHolder* stats_holder, const char* stat_name,
                       HsInt* len, std::string** keys_ptr, int64_t** values_ptr,
                       std::vector<std::string>** keys_,
                       std::vector<int64_t>** values_) {
  return perXStatsGetall<PerStreamStats>(
      stats_holder, &Stats::per_stream_stats, stat_name,
      setPerStreamStatsMember, len, keys_ptr, values_ptr, keys_, values_);
}

int stream_time_series_get(StatsHolder* stats_holder, const char* stat_name,
                           const char* stream_name, HsInt interval_size,
                           HsInt* ms_intervals, HsDouble* aggregate_vals) {
  return perXTimeSeriesGet<
      PerStreamStats, PerStreamTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerStreamStats>>>(
      stats_holder, stat_name, stream_name, setPerStreamTimeSeriesMember,
      &Stats::per_stream_stats, interval_size, ms_intervals, aggregate_vals);
}

// For each thread, for each stream in the thread-local
// per_stream_stats, for each query interval, calculate the rate in B/s and
// aggregate.  Output is a map (stream name, query interval) -> (sum of
// rates collected from different threads).
int stream_time_series_getall_by_name(
    StatsHolder* stats_holder, const char* stat_name,
    //
    HsInt interval_size, HsInt* ms_intervals,
    //
    HsInt* len, std::string** keys_ptr,
    folly::small_vector<double, 4>** values_ptr,
    std::vector<std::string>** keys_,
    std::vector<folly::small_vector<double, 4>>** values_) {
  return perXTimeSeriesGetall<
      PerStreamStats, PerStreamTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerStreamStats>>>(
      stats_holder, stat_name, setPerStreamTimeSeriesMember,
      &Stats::per_stream_stats, interval_size, ms_intervals, len, keys_ptr,
      values_ptr, keys_, values_);
}

// ----------------------------------------------------------------------------
// PerConnectorStats
#define STAT_DEFINE(name, _)                                                   \
  PER_X_STAT_DEFINE(connector_stat_, per_connector_stats, PerConnectorStats, name)
#include "per_connector_stats.inc"

int connector_stat_getall(StatsHolder* stats_holder, const char* stat_name,
                       HsInt* len, std::string** keys_ptr, int64_t** values_ptr,
                       std::vector<std::string>** keys_,
                       std::vector<int64_t>** values_) {
  return perXStatsGetall<PerConnectorStats>(
      stats_holder, &Stats::per_connector_stats, stat_name,
      setPerConnectorStatsMember, len, keys_ptr, values_ptr, keys_, values_);
}

// ----------------------------------------------------------------------------
// PerSubscriptionStats

#define STAT_DEFINE(name, _)                                                   \
  PER_X_STAT_DEFINE(subscription_stat_, per_subscription_stats,                \
                    PerSubscriptionStats, name)
#include "per_subscription_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  PER_X_TIME_SERIES_DEFINE(subscription_time_series_, per_subscription_stats,  \
                           PerSubscriptionStats, PerSubscriptionTimeSeries,    \
                           name)
#include "per_subscription_time_series.inc"

int subscription_stat_getall(StatsHolder* stats_holder, const char* stat_name,
                             HsInt* len, std::string** keys_ptr,
                             int64_t** values_ptr,
                             std::vector<std::string>** keys_,
                             std::vector<int64_t>** values_) {
  return perXStatsGetall<PerSubscriptionStats>(
      stats_holder, &Stats::per_subscription_stats, stat_name,
      setPerSubscriptionStatsMember, len, keys_ptr, values_ptr, keys_, values_);
}

int subscription_time_series_get(StatsHolder* stats_holder,
                                 const char* stat_name, const char* subs_name,
                                 HsInt interval_size, HsInt* ms_intervals,
                                 HsDouble* aggregate_vals) {
  return perXTimeSeriesGet<
      PerSubscriptionStats, PerSubscriptionTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerSubscriptionStats>>>(
      stats_holder, stat_name, subs_name, setPerSubscriptionTimeSeriesMember,
      &Stats::per_subscription_stats, interval_size, ms_intervals,
      aggregate_vals);
}

// For each thread, for each stream in the thread-local
// per_subscription_stats, for each query interval, calculate the rate in B/s
// and aggregate.  Output is a map (subs name, query interval) -> (sum of
// rates collected from different threads).
int subscription_time_series_getall_by_name(
    StatsHolder* stats_holder, const char* stat_name,
    //
    HsInt interval_size, HsInt* ms_intervals,
    //
    HsInt* len, std::string** keys_ptr,
    folly::small_vector<double, 4>** values_ptr,
    std::vector<std::string>** keys_,
    std::vector<folly::small_vector<double, 4>>** values_) {
  return perXTimeSeriesGetall<
      PerSubscriptionStats, PerSubscriptionTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerSubscriptionStats>>>(
      stats_holder, stat_name, setPerSubscriptionTimeSeriesMember,
      &Stats::per_subscription_stats, interval_size, ms_intervals, len,
      keys_ptr, values_ptr, keys_, values_);
}

// ----------------------------------------------------------------------------
// PerHandleStats

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  PER_X_TIME_SERIES_DEFINE(handle_time_series_, per_handle_stats,              \
                           PerHandleStats, PerHandleTimeSeries, name)
#include "per_handle_time_series.inc"

int handle_time_series_get(StatsHolder* stats_holder, const char* stat_name,
                           const char* key_name, HsInt interval_size,
                           HsInt* ms_intervals, HsDouble* aggregate_vals) {
  return perXTimeSeriesGet<
      PerHandleStats, PerHandleTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerHandleStats>>>(
      stats_holder, stat_name, key_name, setPerHandleTimeSeriesMember,
      &Stats::per_handle_stats, interval_size, ms_intervals, aggregate_vals);
}

int handle_time_series_getall_by_name(
    StatsHolder* stats_holder, const char* stat_name,
    //
    HsInt interval_size, HsInt* ms_intervals,
    //
    HsInt* len, std::string** keys_ptr,
    folly::small_vector<double, 4>** values_ptr,
    std::vector<std::string>** keys_,
    std::vector<folly::small_vector<double, 4>>** values_) {
  return perXTimeSeriesGetall<
      PerHandleStats, PerHandleTimeSeries,
      std::unordered_map<std::string, std::shared_ptr<PerHandleStats>>>(
      stats_holder, stat_name, setPerHandleTimeSeriesMember,
      &Stats::per_handle_stats, interval_size, ms_intervals, len, keys_ptr,
      values_ptr, keys_, values_);
}

// ----------------------------------------------------------------------------

// checks that no requested intervals are higher than getMaxInterval
#define VerifyIntervals(perfix, TsCls, getMaxInterval)                         \
  HsBool perfix##verify_intervals(                                             \
      StatsHolder* stats_holder, const char* time_series, HsInt interval_size, \
      HsInt* ms_intervals, std::string** err) {                                \
    auto max_interval =                                                        \
        stats_holder->params_.get()->getMaxInterval(std::string(time_series)); \
    using namespace std::chrono;                                               \
    for (int i = 0; i < interval_size; i++) {                                  \
      auto interval = std::chrono::duration_cast<TsCls::Duration>(             \
          std::chrono::milliseconds{ms_intervals[i]});                         \
      if (interval > max_interval) {                                           \
        *err = /* free on haskell side */                                      \
            new std::string(                                                   \
                (boost::format(                                                \
                     "requested interval %s is larger than the max %s") %      \
                 chrono_string(duration_cast<seconds>(interval)).c_str() %     \
                 chrono_string(duration_cast<seconds>(max_interval)).c_str())  \
                    .str());                                                   \
        return false;                                                          \
      }                                                                        \
    }                                                                          \
    return true;                                                               \
  }
VerifyIntervals(per_stream_, PerStreamTimeSeries, maxStreamStatsInterval);
VerifyIntervals(per_subscription_, PerSubscriptionTimeSeries,
                maxSubscribptionStatsInterval);
VerifyIntervals(per_handle_, PerHandleTimeSeries, maxHandleStatsInterval);

// ----------------------------------------------------------------------------

// TODO: generic Histogram methods

int server_histogram_add(StatsHolder* stats_holder, const char* stat_name,
                         int64_t usecs) {
  if (stats_holder && stats_holder->get().server_histograms) {
    auto histogram =
        stats_holder->get().server_histograms->find(std::string(stat_name));
    if (histogram) {
      histogram->add(usecs);
      return 0;
    }
  }

  return -1;
}

// Computes aggregated sample values
int server_histogram_estimatePercentiles(
    StatsHolder* stats_holder, const char* stat_name,
    const HsDouble* percentiles, size_t npercentiles, int64_t* samples_out,
    uint64_t* count_out, int64_t* sum_out) {
  if (!stats_holder) {
    // Without stats, there is nothing we can do.
    return -1;
  }

  auto agg_hist = stats_holder->aggregate_nonew().server_histograms;
  if (!agg_hist) {
    ld_error("No such server_histograms!");
    return -1;
  }

  auto histogram = agg_hist->find(std::string(stat_name));
  if (!histogram) {
    ld_error("No such server_histograms: %s!", stat_name);
    return -1;
  }

  histogram->estimatePercentiles(percentiles, npercentiles, samples_out,
                                 count_out, sum_out);
  return 0;
}

/**
 * Computes a sample value at the given percentile (must be between 0 and 1).
 * Because we don't keep individual samples but only counts in buckets,
 * we'll know the right bucket but make a linear estimate within it.
 * If histogram is empty, returns 0.
 *
 * Thread-safe.
 *
 * NOTE: This is a fairly expensive function. Prefer estimatePercentiles() to
 *       estimate sample values for a whole batch of percentiles.
 */
// TODO: use param "int* ret_out" as returned code
int64_t server_histogram_estimatePercentile(StatsHolder* stats_holder,
                                            const char* stat_name,
                                            double percentile) {
  if (!stats_holder) {
    // Without stats, there is nothing we can do.
    return -1;
  }

  auto agg_hist = stats_holder->aggregate_nonew().server_histograms;
  if (!agg_hist) {
    ld_error("No such server_histograms!");
    return -1;
  }

  auto histogram = agg_hist->find(std::string(stat_name));
  if (!histogram) {
    ld_error("No such server_histograms: %s!", stat_name);
    return -1;
  }

  return histogram->estimatePercentile(percentile);
}

// ----------------------------------------------------------------------------
}
