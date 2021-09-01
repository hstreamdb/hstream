#include "hs_common.h"
#include "hs_cpp_lib.h"
#include "stats.h"

#include <logdevice/common/debug.h>

extern "C" {
// ----------------------------------------------------------------------------

StatsHolder* new_stats_holder() { return new StatsHolder(StatsParams()); }
void delete_stats_holder(StatsHolder* s) { delete s; }

// TODO: split into a specific aggregate function. e.g.
// new_aggregate_stream_stats
Stats* new_aggregate_stats(StatsHolder* s) { return s->aggregate(); }
void delete_stats(Stats* s) { delete s; }

void stats_holder_print(StatsHolder* s) { s->print(); }

// ----------------------------------------------------------------------------
// PerStreamStats

#define STAT_DEFINE(name, _)                                                   \
  void stream_stat_add_##name(StatsHolder* stats_holder,                       \
                              const char* stream_name, int64_t val) {          \
    if (stats_holder) {                                                        \
      auto stats_struct = &(stats_holder->get());                              \
      STREAM_STAT_ADD(stats_struct, stream_name, name, val);                   \
    }                                                                          \
  }                                                                            \
  int64_t stream_stat_get_##name(Stats* stats, const char* stream_name) {      \
    if (stats) {                                                               \
      auto stats_rlock = stats->per_stream_stats.rlock();                      \
      auto stats_it = stats_rlock->find(std::string(stream_name));             \
      if (stats_it != stats_rlock->end()) {                                    \
        auto r = stats_it->second->name.load();                                \
        if (UNLIKELY(r < 0)) {                                                 \
          ld_error("PerStreamStats overflowed!");                              \
        } else {                                                               \
          return r;                                                            \
        }                                                                      \
      }                                                                        \
    }                                                                          \
    return -1;                                                                 \
  }                                                                            \
  void stream_stat_getall_##name(                                              \
      Stats* stats, HsInt* len, std::string** stream_names_ptr,                \
      int64_t** values_ptr, std::vector<std::string>** keys_,                  \
      std::vector<int64_t>** values_) {                                        \
    if (stats) {                                                               \
      auto& stats_rlock = *(stats->per_stream_stats.rlock());                  \
      /* TODO                                                                  \
      cppMapToHs<                                                              \
          std::unordered_map<std::string, std::shared_ptr<PerStreamStats>>,    \
          std::string, std::shared_ptr<PerStreamStats>,                        \
          std::function<int64_t(std::shared_ptr<PerStreamStats&&>)>>(          \
          stats_rlock, [](auto&& val) { val->name.load(); }, len,              \
          stream_names_ptr, values_ptr, keys_, values_);                       \
      */                                                                       \
      *len = stats_rlock.size();                                               \
      std::vector<std::string>* keys = new std::vector<std::string>;           \
      keys->reserve(*len);                                                     \
      std::vector<int64_t>* values = new std::vector<int64_t>;                 \
      values->reserve(*len);                                                   \
      for (const auto& [key, value] : stats_rlock) {                           \
        keys->push_back(key);                                                  \
        values->push_back(value->name.load());                                 \
      }                                                                        \
      *stream_names_ptr = keys->data();                                        \
      *values_ptr = values->data();                                            \
      *keys_ = keys;                                                           \
      *values_ = values;                                                       \
    }                                                                          \
  }
#include "per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  void stream_time_series_add_##name(StatsHolder* stats_holder,                \
                                     const char* stream_name, int64_t val) {   \
    if (stats_holder) {                                                        \
      auto stats_struct = &(stats_holder->get());                              \
      STREAM_TIME_SERIES_ADD(stats_struct, stream_name, name, val);            \
    }                                                                          \
  }
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

  *len = output.size();
  auto keys = new std::vector<std::string>;
  keys->reserve(*len);
  auto values = new std::vector<folly::small_vector<double, 4>>;
  values->reserve(*len);
  for (const auto& [key, value] : output) {
    keys->push_back(key);
    values->push_back(value);
  }
  *keys_ptr = keys->data();
  *values_ptr = values->data();
  *keys_ = keys;
  *values_ = values;
}

// TODO
// bool verifyIntervals(StatsHolder* stats_holder, std::string string_name,
//                     std::vector<Duration> query_intervals, std::string& err)
//                     {
//  Duration max_interval =
//  stats_holder->params_.get()->maxInterval(string_name); using namespace
//  std::chrono; for (auto interval : query_intervals) {
//    if (interval > max_interval) {
//      err = (boost::format("requested interval %s is larger than the max %s")
//      %
//             chrono_string(duration_cast<seconds>(interval)).c_str() %
//             chrono_string(duration_cast<seconds>(max_interval)).c_str())
//                .str();
//      return false;
//    }
//  }
//  return true;
//}

// ----------------------------------------------------------------------------
}
