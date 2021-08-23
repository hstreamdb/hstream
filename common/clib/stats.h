#pragma once

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/stats/BucketedTimeSeries.h>
#include <folly/stats/MultiLevelTimeSeries.h>

#include <logdevice/common/checks.h>
#include <logdevice/common/stats/Stats.h>
#include <logdevice/common/stats/StatsCounter.h>

// ----------------------------------------------------------------------------

using facebook::logdevice::StatsAgg;
using facebook::logdevice::StatsAggOptional;
using facebook::logdevice::StatsCounter;

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

#define STAT_DEFINE(name, _) StatsCounter name{};
#include "per_stream_stats.inc"
  void aggregate(PerStreamStats const& other, StatsAggOptional agg_override);
  // Show all per_stream_stats to a json formatted string.
  folly::dynamic toJsonObj();
  std::string toJson();

#define TIME_SERIES_DEFINE(name, _, t, buckets)                                \
  std::vector<std::chrono::milliseconds> time_intervals_##name = t;            \
  size_t num_buckets_##name = buckets;                                         \
  std::shared_ptr<PerStreamTimeSeries> name;
#include "per_stream_time_series.inc"

  // Mutex almost exclusively locked by one thread since PerStreamStats
  // objects are contained in thread-local stats
  std::mutex mutex;
};

// ----------------------------------------------------------------------------
// All Stats

struct Stats {

  explicit Stats();

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
};

/**
 * StatsHolder wraps multiple (thread-local) instances of Stats objects. It
 * supports aggregation and resetting. StatsHolder::get() method should be
 * used to obtain a Stats object local to the current os thread.
 */
class StatsHolder {
public:
  explicit StatsHolder();
  ~StatsHolder();

  /**
   * Collect stats from all threads.
   */
  Stats aggregate() const;

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

  explicit StatsWrapper(StatsHolder* owner) : stats(), owner(owner) {}

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

#define STREAM_STAT_ADD(stats_struct, stream_name, stat_name, val)             \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->per_stream_stats.ulock();             \
      auto stats_it = stats_ulock->find((stream_name));                        \
      if (stats_it != stats_ulock->end()) {                                    \
        /* PerStreamStats for stream_name already exist (common case). */      \
        /* Just atomically increment the value.  */                            \
        stats_it->second->stat_name += (val);                                  \
      } else {                                                                 \
        /* PerStreamStats for stream_name do not exist yet (rare case). */     \
        /* Upgrade ulock to wlock and emplace new PerStreamStats. */           \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<PerStreamStats>();                   \
        stats_ptr->stat_name += (val);                                         \
        stats_ulock.moveFromUpgradeToWrite()->emplace_hint(                    \
            stats_it, (stream_name), std::move(stats_ptr));                    \
      }                                                                        \
    }                                                                          \
  } while (0)

#define STREAM_TIME_SERIES_ADD(stats_struct, stream_name, stat_name, val)      \
  do {                                                                         \
    if (stats_struct) {                                                        \
      auto stats_ulock = (stats_struct)->per_stream_stats.ulock();             \
      /* Unfortunately, the type of the lock after a downgrade from write to   \
       * upgrade isn't the same as the type of upgrade lock initially acquired \
       */                                                                      \
      folly::LockedPtr<decltype(stats_ulock)::Synchronized,                    \
                       folly::LockPolicyFromExclusiveToUpgrade>                \
          stats_downgraded_ulock;                                              \
      auto stats_it = stats_ulock->find((stream_name));                        \
      if (UNLIKELY(stats_it == stats_ulock->end())) {                          \
        /* PerStreamStats for stream_name do not exist yet (rare case). */     \
        /* Upgrade ulock to wlock and emplace new PerStreamStats. */           \
        /* No risk of deadlock because we are the only writer thread. */       \
        auto stats_ptr = std::make_shared<PerStreamStats>();                   \
        auto stats_wlock = stats_ulock.moveFromUpgradeToWrite();               \
        stats_it =                                                             \
            stats_wlock->emplace((stream_name), std::move(stats_ptr)).first;   \
        stats_downgraded_ulock = stats_wlock.moveFromWriteToUpgrade();         \
      }                                                                        \
      {                                                                        \
        std::lock_guard<std::mutex> guard(stats_it->second->mutex);            \
        if (UNLIKELY(!stats_it->second->stat_name)) {                          \
          stats_it->second->stat_name = std::make_shared<PerStreamTimeSeries>( \
              stats_it->second->num_buckets_##stat_name,                       \
              stats_it->second->time_intervals_##stat_name);                   \
        }                                                                      \
        stats_it->second->stat_name->addValue(val);                            \
      }                                                                        \
    }                                                                          \
  } while (0)
