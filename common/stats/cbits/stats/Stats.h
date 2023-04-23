#pragma once

#include <boost/core/demangle.hpp>
#include <folly/dynamic.h>
#include <folly/experimental/StringKeyedUnorderedMap.h>
#include <folly/json.h>
#include <folly/small_vector.h>
#include <folly/stats/BucketedTimeSeries.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <logdevice/common/UpdateableSharedPtr.h>
#include <logdevice/common/checks.h>
#include <logdevice/common/stats/Stats.h>
#include <logdevice/common/stats/StatsCounter.h>

#include <hs_common.h>
#include <hs_cpp_lib.h>

#include "ServerHistograms.h"

// ----------------------------------------------------------------------------

using facebook::logdevice::FastUpdateableSharedPtr;
using facebook::logdevice::StatsAgg;
using facebook::logdevice::StatsAggOptional;
using facebook::logdevice::StatsCounter;

namespace hstream { namespace common {

/**
 * A simple wrapper for folly::MultiLevelTimeSeries, which support addValue for
 * current time.
 */
template <typename VT,
          typename CT = folly::LegacyStatsClock<std::chrono::milliseconds>>
struct MultiLevelTimeSeriesWrapper
    : public folly::MultiLevelTimeSeries<VT, CT> {
  using ValueType = VT;
  using Duration = typename CT::duration;
  using TimePoint = typename CT::time_point;

  MultiLevelTimeSeriesWrapper() = default;

  MultiLevelTimeSeriesWrapper(
      size_t num_buckets,
      std::vector<std::chrono::milliseconds> time_intervals);

  ~MultiLevelTimeSeriesWrapper() = default;

  void addValue(const ValueType& val);
};

template <typename VT, typename CT>
MultiLevelTimeSeriesWrapper<VT, CT>::MultiLevelTimeSeriesWrapper(
    size_t num_buckets, std::vector<std::chrono::milliseconds> time_intervals)
    : folly::MultiLevelTimeSeries<VT, CT>(num_buckets, time_intervals.size(),
                                          time_intervals.data()) {}

template <typename VT, typename CT>
void MultiLevelTimeSeriesWrapper<VT, CT>::addValue(const ValueType& n) {
  auto now = std::chrono::duration_cast<Duration>(
      std::chrono::steady_clock::now().time_since_epoch());
  folly::MultiLevelTimeSeries<VT, CT>::addValue(now, n);
}

// ----------------------------------------------------------------------------
// PerStreamStats

using PerStreamTimeSeries = MultiLevelTimeSeriesWrapper<int64_t>;

struct PerStreamStats {
#define STAT_DEFINE(name, _) StatsCounter name##_counter{};
#include "per_stream_stats.inc"
  void aggregate(PerStreamStats const& other, StatsAggOptional agg_override);
  // Show all per_stream_stats to a json formatted string.
  folly::dynamic toJsonObj();
  std::string toJson();

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::shared_ptr<PerStreamTimeSeries> name;
#include "per_stream_time_series.inc"

  // Mutex almost exclusively locked by one thread since PerStreamStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// PerConnectorStats
//
struct PerConnectorStats {
#define STAT_DEFINE(name, _) StatsCounter name##_counter{};
#include "per_connector_stats.inc"
  void aggregate(PerConnectorStats const& other, StatsAggOptional agg_override);
  // Show all per_connector_stats to a json formatted string.
  folly::dynamic toJsonObj();
  std::string toJson();

  // Mutex almost exclusively locked by one thread since PerConnectorStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// PerQueryStats
//
struct PerQueryStats {
#define STAT_DEFINE(name, _) StatsCounter name##_counter{};
#include "per_query_stats.inc"
  void aggregate(PerQueryStats const& other, StatsAggOptional agg_override);
  // Show all per_query_stats to a json formatted string.
  folly::dynamic toJsonObj();
  std::string toJson();

  // Mutex almost exclusively locked by one thread since PerQueryStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// PerSubscriptionStats

using PerSubscriptionTimeSeries = MultiLevelTimeSeriesWrapper<int64_t>;

struct PerSubscriptionStats {
#define STAT_DEFINE(name, _) StatsCounter name##_counter{};
#include "per_subscription_stats.inc"
  void aggregate(PerSubscriptionStats const& other,
                 StatsAggOptional agg_override);
  // Show all per_stream_stats to a json formatted string.
  folly::dynamic toJsonObj();
  std::string toJson();

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::shared_ptr<PerSubscriptionTimeSeries> name;
#include "per_subscription_time_series.inc"

  // Mutex almost exclusively locked by one thread since PerSubscriptionStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// PerHandleStats

using PerHandleTimeSeries = MultiLevelTimeSeriesWrapper<int64_t>;

struct PerHandleStats {
#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::shared_ptr<PerHandleTimeSeries> name;
#include "per_handle_time_series.inc"

  // Mutex almost exclusively locked by one thread since PerHandleStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// All Stats

struct StatsParams {
  explicit StatsParams() = default;

  bool is_server = false;

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::vector<std::chrono::milliseconds> time_intervals_##name = t;            \
  size_t num_buckets_##name = buckets;
#include "per_stream_time_series.inc"

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::vector<std::chrono::milliseconds> time_intervals_##name = t;            \
  size_t num_buckets_##name = buckets;
#include "per_subscription_time_series.inc"

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::vector<std::chrono::milliseconds> time_intervals_##name = t;            \
  size_t num_buckets_##name = buckets;
#include "per_handle_time_series.inc"

  // Get MaxInterval of StreamStats by string(command) name
  PerStreamTimeSeries::Duration maxStreamStatsInterval(std::string string_name);
  // MaxInterval of SubscriptionStats
  PerSubscriptionTimeSeries::Duration
  maxSubscribptionStatsInterval(std::string string_name);
  // MaxInterval of HandleStats(PerHandleTimeSeries)
  PerStreamTimeSeries::Duration maxHandleStatsInterval(std::string string_name);

  // Below here are the setters for the above member variables

  StatsParams& setIsServer(bool is_server) {
    this->is_server = is_server;
    return *this;
  }
};

struct Stats {

  explicit Stats(const FastUpdateableSharedPtr<StatsParams>* params);

  ~Stats();

  /**
   * Copy constructor and copy-assignment. Thread-safe.
   */
  Stats(const Stats& other);
  Stats& operator=(const Stats& other);

  /**
   * Move constructor and move-assignment. Not thread-safe (but only used in
   * unit tests, so okay).
   */
  Stats(Stats&& other) noexcept(true);
  Stats& operator=(Stats&& other) noexcept(false);

  /**
   * Add all values from @param other.
   */
  void aggregate(Stats const& other,
                 StatsAggOptional agg_override = folly::none);

  /**
   * Same but with DESTROYING_THREAD defined, i.e. exclude stats which
   * should only be accumulated for living threads.
   */
  void aggregateForDestroyedThread(Stats const& other);

  /**
   * Same but only for per-something stats.
   * Only used internally by aggregate() and aggregateForDestroyedThread().
   */
  void aggregateCompoundStats(Stats const& other, StatsAggOptional agg_override,
                              bool destroyed_threads = false);

  /**
   * TODO
   *
   * Calculates derived stats based on non-derived stats.
   * Derived stats are declared in *stats.inc files like the other stats, and
   * are handled the same way. The difference is that their values are set
   * *after* aggregating thread-local stats from all threads (see
   * StatsHolder::aggregate()). This function calculates all derived stats.
   */
  void deriveStats();

  /**
   * Reset all counters to their initial values.
   */
  void reset();

  /**
   * Show all stats to a json formatted string.
   */
  folly::dynamic toJsonObj();
  std::string toJson();

  /**
   * Take a read-lock and make a deep copy of a some map wrapped in a
   * folly::Synchronized, such as per_stream_stats.
   */
  template <typename Map>
  auto synchronizedCopy(folly::Synchronized<Map> Stats::*map) const {
    return (this->*map).withRLock([](const auto& locked_map) {
      return std::vector<
          std::pair<typename Map::key_type, typename Map::mapped_type>>(
          locked_map.begin(), locked_map.end());
    });
  }

  // Per-stream stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerStreamStats>>>
      per_stream_stats;

  // Per-connector stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerConnectorStats>>>
      per_connector_stats;

  // Per-query stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerQueryStats>>>
      per_query_stats;

  // Per-subscription stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerSubscriptionStats>>>
      per_subscription_stats;

  // Per-handle stats
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PerHandleStats>>>
      per_handle_stats;

  // Server histograms.
  std::unique_ptr<ServerHistograms> server_histograms;

  const FastUpdateableSharedPtr<StatsParams>* params;

  /**
   * Indicates whether we're representing server stats.
   */
  bool isServerStats() const { return params->get()->is_server; }
};

/**
 * StatsHolder wraps multiple (thread-local) instances of Stats objects. It
 * supports aggregation and resetting. StatsHolder::get() method should be
 * used to obtain a Stats object local to the current os thread.
 */
class StatsHolder {
public:
  explicit StatsHolder(StatsParams params);
  ~StatsHolder();

  /**
   * Collect stats from all threads. It's your duty to delete the returned ptr.
   */
  Stats* aggregate() const;

  Stats aggregate_nonew() const;

  void aggregateStreamTimeSeries();

  /**
   * Reset stats on all threads.
   */
  void reset();

  /**
   * Returns the Stats object for the current thread.
   */
  inline Stats& get();

  /**
   * Print all thread's Stats to stdout, useful for debugging.
   */
  void print();

  /**
   * Executes a function on each thread's Stats object.
   */
  template <typename Func> void runForEach(const Func& func);

  FastUpdateableSharedPtr<StatsParams> params_;

private:
  // Destructor adds stats to dead_stats_.
  struct StatsWrapper;
  struct Tag;

  // Stats aggregated for all destroyed threads.
  Stats dead_stats_;

  // Stats for running threads.
  //
  // We use AccessModeStrict to prevent race conditions around dead_stats_;
  // full explanation below.
  //
  // folly::ThreadLocal contains the list of thread-local instances. The list is
  // accessible through accessAllThreads(). There's a mutex protecting the
  // list. accessAllThreads() locks the mutex for the duration of the iteration.
  // When a new thread is added or an existing thread exits, it locks the mutex
  // and adds/removes itself to/from the list.
  //
  // In strict mode, when a thread is exiting, destructor of the thread-local
  // instance (~StatsWrapper() in our case) is called while the mutex is still
  // locked. In non-strict mode, the mutex is unlocked before calling the
  // destructor.
  //
  // Using non-strict mode here would cause the following race conditions:
  //  1. Thread A is exiting; it locks the mutex, removes itself from the list,
  //     unlocks the mutes, then hesitates for a bit; ~StatsHolder() runs and
  //     returns, unsetting `owner` on all threads except A (which is already
  //     removed from the list); StatsHolder is deallocated; then A proceeds to
  //     call ~StatsWrapper(), which tries to access the destroyed StatsHolder.
  //     (And there's also the trivial data race on the `owner` field itself.)
  //  2. Thread A is exiting; it removes itself from the list and hesitates for
  //     a bit; the stats from A are neither in thread_stats_ nor in
  //     dead_stats_; StatsHolder::aggregate() runs and doesn't see A's stats;
  //     then A proceeds to update dead_stats_, and the next aggregate() call
  //     sees A's stats again.
  //
  // In strict mode, folly::ThreadLocal's mutex effectively protects both
  // thread_stats_'s list of threads and dead_stats_ together, so in
  // aggregate()'s view each thread's stats are accounted once: either in
  // dead_stats_ or in thread_stats_.
  folly::ThreadLocalPtr<StatsWrapper, Tag, folly::AccessModeStrict>
      thread_stats_;
};

struct StatsHolder::StatsWrapper {
  Stats stats;
  StatsHolder* owner;

  explicit StatsWrapper(StatsHolder* owner)
      : stats(&owner->params_), owner(owner) {}

  ~StatsWrapper() {
    if (owner) {
      owner->dead_stats_.aggregateForDestroyedThread(stats);
    }
  }
};

Stats& StatsHolder::get() {
  StatsWrapper* wrapper = thread_stats_.get();
  if (!wrapper) {
    wrapper = new StatsWrapper(this);
    thread_stats_.reset(wrapper);
  }
  return wrapper->stats;
}

template <typename Func> void StatsHolder::runForEach(const Func& func) {
  auto accessor = thread_stats_.accessAllThreads();
  func(dead_stats_);
  for (auto& x : accessor) {
    func(x.stats);
  }
}

// ----------------------------------------------------------------------------

#define PER_X_STAT_ADD(stats_struct, x, x_ty, stat_name, key, val)             \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->get().x.ulock();                      \
      auto stats_it = stats_ulock->find((key));                                \
      if (stats_it != stats_ulock->end()) {                                    \
        /* x_ty for key already exist (common case). */                        \
        /* Just atomically increment the value.  */                            \
        stats_it->second->stat_name##_counter += (val);                        \
      } else {                                                                 \
        /* x_ty for key do not exist yet (rare case). */                       \
        /* Upgrade ulock to wlock and emplace new x_ty. */                     \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<x_ty>();                             \
        stats_ptr->stat_name##_counter += (val);                               \
        stats_ulock.moveFromUpgradeToWrite()->emplace_hint(                    \
            stats_it, (key), std::move(stats_ptr));                            \
      }                                                                        \
    }                                                                          \
  } while (0)

#define PER_X_STAT_GET(stats_agg, x, stat_name, key)                           \
  do {                                                                         \
    if (stats_agg) {                                                           \
      auto stats_rlock = stats_agg->x.rlock();                                 \
      auto stats_it = stats_rlock->find(std::string(key));                     \
      if (stats_it != stats_rlock->end()) {                                    \
        auto r = stats_it->second->stat_name##_counter.load();                 \
        if (UNLIKELY(r < 0)) {                                                 \
          ld_error("PerStreamStats overflowed!");                              \
        } else {                                                               \
          return r;                                                            \
        }                                                                      \
      }                                                                        \
    }                                                                          \
    return -1;                                                                 \
  } while (0)

#define PER_X_TIME_SERIES_ADD(stats_struct, x, x_ty, stat_name, stat_name_ty,  \
                              key, val)                                        \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->get().x.ulock();                      \
      /* Unfortunately, the type of the lock after a downgrade from write to   \
       * upgrade isn't the same as the type of upgrade lock initially acquired \
       */                                                                      \
      folly::LockedPtr<decltype(stats_ulock)::Synchronized,                    \
                       folly::detail::SynchronizedLockPolicyUpgrade>           \
          stats_downgraded_ulock;                                              \
      auto stats_it = stats_ulock->find((key));                                \
      if (UNLIKELY(stats_it == stats_ulock->end())) {                          \
        /* x_ty for key do not exist yet (rare case). */                       \
        /* Upgrade ulock to wlock and emplace new x_ty. */                     \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<x_ty>();                             \
        auto stats_wlock = stats_ulock.moveFromUpgradeToWrite();               \
        stats_it = stats_wlock->emplace((key), std::move(stats_ptr)).first;    \
        stats_downgraded_ulock = stats_wlock.moveFromWriteToUpgrade();         \
      }                                                                        \
      {                                                                        \
        std::lock_guard<std::mutex> guard(stats_it->second->mutex);            \
        if (UNLIKELY(!stats_it->second->stat_name)) {                          \
          stats_it->second->stat_name = std::make_shared<stat_name_ty>(        \
              (stats_struct)->params_.get()->num_buckets_##stat_name,          \
              (stats_struct)->params_.get()->time_intervals_##stat_name);      \
        }                                                                      \
        stats_it->second->stat_name->addValue(val);                            \
      }                                                                        \
    }                                                                          \
  } while (0)

template <class PerXStats, class PerXTimeSeries, typename Map>
int perXTimeSeriesGet(
    StatsHolder* stats_holder, const char* stat_name, const char* key,
    //
    std::function<void(const char*,
                       std::shared_ptr<PerXTimeSeries> PerXStats::*&)>
        find_member,
    folly::Synchronized<Map> Stats::*stats_member_map,
    //
    HsInt interval_size, HsInt* ms_intervals, HsDouble* aggregate_vals) {
  using Duration = typename PerXTimeSeries::Duration;
  using TimePoint = typename PerXTimeSeries::TimePoint;

  std::shared_ptr<PerXTimeSeries> PerXStats::*member_ptr = nullptr;
  find_member(stat_name, member_ptr);
  // TODO: also return failure reasons
  if (UNLIKELY(member_ptr == nullptr)) {
    return -1;
  }

  bool has_found = false;
  stats_holder->runForEach([&](Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_x_stats map while we iterate over individual entries.
    for (auto& entry : s.synchronizedCopy(stats_member_map)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);

      std::string& key_ = entry.first;
      auto time_series = entry.second.get()->*member_ptr;
      if (!time_series) {
        continue;
      }

      if (key_ == std::string(key)) {
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
              time_series->template rate<double>(now - interval, now);
          // Duration may not be seconds, convert to seconds
          aggregate_vals[i] += rate_per_time_type * Duration::period::den /
                               Duration::period::num;
        }

        // We have aggregated the stat from this Stats, because stream name
        // in Stats are unique(as keys of unordered_map), we can break the loop
        // safely.
        if (!has_found) {
          has_found = true;
        }
        break;
      }
    }
  });

  return has_found ? 0 : -1;
}

template <class PerXStats, class PerXTimeSeries, typename Map>
int perXTimeSeriesGetall(
    StatsHolder* stats_holder, const char* stat_name,
    //
    std::function<void(const char*,
                       std::shared_ptr<PerXTimeSeries> PerXStats::*&)>
        find_member,
    folly::Synchronized<Map> Stats::*stats_member_map,
    //
    HsInt interval_size, HsInt* ms_intervals,
    //
    HsInt* len, std::string** keys_ptr,
    folly::small_vector<double, 4>** values_ptr,
    std::vector<std::string>** keys_,
    std::vector<folly::small_vector<double, 4>>** values_) {

  using Duration = typename PerXTimeSeries::Duration;
  using TimePoint = typename PerXTimeSeries::TimePoint;
  using AggregateMap =
      folly::StringKeyedUnorderedMap<folly::small_vector<double, 4>>;

  AggregateMap output;

  std::shared_ptr<PerXTimeSeries> PerXStats::*member_ptr = nullptr;
  find_member(stat_name, member_ptr);
  // TODO: also return failure reasons
  if (UNLIKELY(member_ptr == nullptr)) {
    return -1;
  }

  stats_holder->runForEach([&](Stats& s) {
    // Use synchronizedCopy() so we do not have to hold a read lock on
    // per_log_stats map while we iterate over individual entries.
    for (auto& entry : s.synchronizedCopy(stats_member_map)) {
      std::lock_guard<std::mutex> guard(entry.second->mutex);

      std::string& key = entry.first;
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

      auto& aggregate_vector = output[key];
      aggregate_vector.resize(interval_size);
      // For each query interval, make a MultiLevelTimeSeries::rate() call
      // to find the approximate rate over that interval
      for (int i = 0; i < interval_size; ++i) {
        const Duration interval = std::chrono::duration_cast<Duration>(
            std::chrono::milliseconds{ms_intervals[i]});

        auto rate_per_time_type =
            time_series->template rate<double>(now - interval, now);
        // Duration may not be seconds, convert to seconds
        aggregate_vector[i] +=
            rate_per_time_type * Duration::period::den / Duration::period::num;
      }
    }
  });

  cppMapToHs<AggregateMap, std::string, folly::small_vector<double, 4>,
             std::nullptr_t, std::nullptr_t>(
      output, nullptr, nullptr, len, keys_ptr, values_ptr, keys_, values_);

  return 0;
}

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
          [](auto&& val) { return val->stat_name##_counter.load(); }, len,     \
          keys_ptr, values_ptr, keys_, values_);                               \
    }                                                                          \
  }

// TODO: return errmsg
template <class PerXStats>
int perXStatsGetall(
    StatsHolder* stats_holder,
    //
    folly::Synchronized<
        std::unordered_map<std::string, std::shared_ptr<PerXStats>>>
        Stats::*stats_member_map,
    //
    const char* stat_name,
    std::function<void(const char*, StatsCounter PerXStats::*&)> find_member,
    //
    HsInt* len, std::string** keys_ptr, int64_t** values_ptr,
    std::vector<std::string>** keys_, std::vector<int64_t>** values_) {
  if (!stats_holder) {
    ld_error("No such stasts_holder");
    return -1;
  }
  Stats stats = stats_holder->aggregate_nonew();
  StatsCounter PerXStats::*member_ptr = nullptr;
  find_member(stat_name, member_ptr);
  // TODO: also return failure reasons
  if (UNLIKELY(member_ptr == nullptr)) {
    ld_error("Can't find stats <%s> member: %s \n",
             boost::core::demangle(typeid(PerXStats).name()).c_str(),
             stat_name);
    return -1;
  }

  auto& stats_rlock = *((stats.*stats_member_map).rlock());
  cppMapToHs<std::unordered_map<std::string, std::shared_ptr<PerXStats>>,
             std::string, int64_t, std::nullptr_t,
             std::function<int64_t(std::shared_ptr<PerXStats>)>&&>(
      stats_rlock, nullptr,
      [&member_ptr](auto&& val) { return (val.get()->*member_ptr).load(); },
      len, keys_ptr, values_ptr, keys_, values_);

  return 0;
}

#define PER_X_TIME_SERIES_DEFINE(prefix, x, x_ty, ts_ty, stat_name)            \
  void prefix##add_##stat_name(StatsHolder* stats_holder, const char* key,     \
                               int64_t val) {                                  \
    PER_X_TIME_SERIES_ADD(stats_holder, x, x_ty, stat_name, ts_ty,             \
                          std::string(key), val);                              \
  }

}} // namespace hstream::common
