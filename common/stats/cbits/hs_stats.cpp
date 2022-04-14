#include "hs_stats.h"

// ----------------------------------------------------------------------------

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

// For each thread, for each stream in the thread-local
// per_stream_stats, for each query interval, calculate the rate in B/s and
// aggregate.  Output is a map (stream name, query interval) -> (sum of
// rates collected from different threads).
void stream_time_series_getall_by_name(
    StatsHolder* stats_holder, const char* string_name,
    //
    HsInt interval_size, HsInt* ms_intervals,
    //
    HsInt* len, std::string** keys_ptr,
    folly::small_vector<double, 4>** values_ptr,
    std::vector<std::string>** keys_,
    std::vector<folly::small_vector<double, 4>>** values_) {

  using Duration = PerStreamTimeSeries::Duration;
  using TimePoint = PerStreamTimeSeries::TimePoint;
  using AggregateMap =
      folly::StringKeyedUnorderedMap<folly::small_vector<double, 4>>;
  AggregateMap output;

  std::shared_ptr<PerStreamTimeSeries> PerStreamStats::*member_ptr = nullptr;
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == std::string(string_name)) {                                     \
      member_ptr = &PerStreamStats::name;                                      \
      break;                                                                   \
    }                                                                          \
  }
#include "per_stream_time_series.inc"
  // FIXME: we should not abort in cpp side, instead we should check
  // this in haskell
  ld_check(member_ptr != nullptr);

  stats_holder->runForEach([&](Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_stats map while we iterate over individual entries.
    for (auto& entry : s.synchronizedCopy(&Stats::per_stream_stats)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);

      std::string& stream_name = entry.first;
      auto time_series = entry.second.get()->*member_ptr;
      if (!time_series) {
        continue;
      }

      // NOTE: It might be tempting to pull `now' out of the loops but
      // folly::MultiLevelTimeSeries barfs if we ask it for data that is
      // too old.  Keep it under the lock for now, optimize if necessary.
      //
      // TODO: Constructing the TimePoint is slightly awkward at the moment
      // as the folly stats code is being cleaned up to better support real
      // clock types.  appendBytesTimeSeries_ should simply be changed to
      // use std::steady_clock as it's clock type.  I'll do that in a
      // separate diff for now, though.
      const TimePoint now{std::chrono::duration_cast<Duration>(
          std::chrono::steady_clock::now().time_since_epoch())};
      // Flush any cached updates and discard any stale data
      time_series->update(now);

      auto& aggregate_vector = output[stream_name];
      aggregate_vector.resize(interval_size);
      // For each query interval, make a MultiLevelTimeSeries::rate() call
      // to find the approximate rate over that interval
      for (int i = 0; i < interval_size; ++i) {
        const Duration interval = std::chrono::duration_cast<Duration>(
            std::chrono::milliseconds{ms_intervals[i]});

        auto rate_per_time_type =
            time_series->rate<double>(now - interval, now);
        // Duration may not be seconds, convert to seconds
        aggregate_vector[i] +=
            rate_per_time_type * Duration::period::den / Duration::period::num;
      }
    }
  });

  cppMapToHs<AggregateMap, std::string, folly::small_vector<double, 4>,
             std::nullptr_t, std::nullptr_t>(
      output, nullptr, nullptr, len, keys_ptr, values_ptr, keys_, values_);
}

int stream_time_series_get(StatsHolder* stats_holder, const char* string_name,
                           const char* stream_name_,
                           //
                           HsInt interval_size, HsInt* ms_intervals,
                           HsDouble* aggregate_vals) {
  using Duration = PerStreamTimeSeries::Duration;
  using TimePoint = PerStreamTimeSeries::TimePoint;

  std::shared_ptr<PerStreamTimeSeries> PerStreamStats::*member_ptr = nullptr;
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == std::string(string_name)) {                                     \
      member_ptr = &PerStreamStats::name;                                      \
      break;                                                                   \
    }                                                                          \
  }
#include "per_stream_time_series.inc"
  // TODO: also return failure reasons
  if (UNLIKELY(member_ptr == nullptr)) {
    return -1;
  }

  bool has_found = false;
  stats_holder->runForEach([&](Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_stats map while we iterate over individual entries.
    for (auto& entry : s.synchronizedCopy(&Stats::per_stream_stats)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);

      std::string& stream_name = entry.first;
      auto time_series = entry.second.get()->*member_ptr;
      if (!time_series) {
        continue;
      }

      if (stream_name == std::string(stream_name_)) {
        // NOTE: It might be tempting to pull `now' out of the loops but
        // folly::MultiLevelTimeSeries barfs if we ask it for data that is
        // too old.  Keep it under the lock for now, optimize if necessary.
        //
        // TODO: Constructing the TimePoint is slightly awkward at the moment
        // as the folly stats code is being cleaned up to better support real
        // clock types.  appendBytesTimeSeries_ should simply be changed to
        // use std::steady_clock as it's clock type.  I'll do that in a
        // separate diff for now, though.
        const TimePoint now{std::chrono::duration_cast<Duration>(
            std::chrono::steady_clock::now().time_since_epoch())};
        // Flush any cached updates and discard any stale data
        time_series->update(now);

        // For each query interval, make a MultiLevelTimeSeries::rate() call
        // to find the approximate rate over that interval
        for (int i = 0; i < interval_size; ++i) {
          const Duration interval = std::chrono::duration_cast<Duration>(
              std::chrono::milliseconds{ms_intervals[i]});

          auto rate_per_time_type =
              time_series->rate<double>(now - interval, now);
          // Duration may not be seconds, convert to seconds
          aggregate_vals[i] += rate_per_time_type * Duration::period::den /
                               Duration::period::num;
        }

        // We have aggregated the stat from this Stats, because stream name
        // in Stats are unique(as keys of unordered_map), we can break the loop
        // safely.
        if (!has_found)
          has_found = true;
        break;
      }
    }
  });

  if (has_found)
    return 0;
  else
    return -1;
}

// ----------------------------------------------------------------------------
// PerSubscriptionStats

#define STAT_DEFINE(name, _)                                                   \
  PER_X_STAT_DEFINE(subscription_stat_, per_subscription_stats,                \
                    PerSubscriptionStats, name)
#include "per_subscription_stats.inc"

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
