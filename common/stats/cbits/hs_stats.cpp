#include "hs_stats.h"

extern "C" {
// ----------------------------------------------------------------------------

StatsHolder* new_stats_holder() { return new StatsHolder(StatsParams()); }
void delete_stats_holder(StatsHolder* s) { delete s; }

// TODO: split into a specific aggregate function. e.g.
// new_aggregate_stream_stats
Stats* new_aggregate_stats(StatsHolder* s) { return s->aggregate(); }
void delete_stats(Stats* s) { delete s; }

void stats_holder_print(StatsHolder* s) { s->print(); }

#define PER_X_STAT_DEFINE(prefix, x, x_ty, stat_name)                          \
  void prefix##add_##stat_name(StatsHolder* stats_holder, const char* key,     \
                               int64_t val) {                                  \
    PER_X_STAT_ADD(stats_holder, x, x_ty, stat_name, key, val);                \
  }                                                                            \
  int64_t prefix##get_##stat_name(Stats* stats, const char* key) {             \
    PER_X_STAT_GET(stats, x, stat_name, key);                                  \
  }                                                                            \
  void prefix##getall_##stat_name(                                             \
      Stats* stats, HsInt* len, std::string** keys_ptr, int64_t** values_ptr,  \
      std::vector<std::string>** keys_, std::vector<int64_t>** values_) {      \
    if (stats) {                                                               \
      auto& stats_rlock = *(stats->x.rlock());                                 \
      cppMapToHs<std::unordered_map<std::string, std::shared_ptr<x_ty>>,       \
                 std::string, int64_t, std::nullptr_t,                         \
                 std::function<int64_t(std::shared_ptr<x_ty>)>&&>(             \
          stats_rlock, nullptr,                                                \
          [](auto&& val) { return val->stat_name.load(); }, len, keys_ptr,     \
          values_ptr, keys_, values_);                                         \
    }                                                                          \
  }

#define PER_X_TIME_SERIES_DEFINE(prefix, x, x_ty, ts_ty, stat_name)            \
  void prefix##add_##stat_name(StatsHolder* stats_holder, const char* key,     \
                               int64_t val) {                                  \
    PER_X_TIME_SERIES_ADD(stats_holder, x, x_ty, stat_name, ts_ty,             \
                          std::string(key), val);                              \
  }

// ----------------------------------------------------------------------------
// PerStreamStats

#define STAT_DEFINE(name, _)                                                   \
  PER_X_STAT_DEFINE(stream_stat_, per_stream_stats, PerStreamStats, name)
#include "per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  PER_X_TIME_SERIES_DEFINE(stream_time_series_, per_stream_stats,              \
                           PerStreamStats, PerStreamTimeSeries, name)
#include "per_stream_time_series.inc"

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

/* TODO
bool verifyIntervals(StatsHolder* stats_holder, std::string string_name,
                     std::vector<Duration> query_intervals, std::string& err) {
  Duration max_interval =
      stats_holder->params_.get()->maxStreamStatsInterval(string_name);
  using namespace std::chrono;
  for (auto interval : query_intervals) {
    if (interval > max_interval) {
      err = (boost::format("requested interval %s is larger than the max %s") %
             chrono_string(duration_cast<seconds>(interval)).c_str() %
             chrono_string(duration_cast<seconds>(max_interval)).c_str())
                .str();
      return false;
    }
  }
  return true;
}
*/

// ----------------------------------------------------------------------------
}
