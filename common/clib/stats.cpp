#include "stats.h"

#include <folly/String.h>
#include <iostream>

// ----------------------------------------------------------------------------

static void aggregateStat(StatsAgg agg, StatsCounter& out, int64_t in) {
  switch (agg) {
  case StatsAgg::SUM:
    out += in;
    break;
  case StatsAgg::MAX:
    if (in > out) {
      out = in;
    }
    break;
  case StatsAgg::SUBTRACT:
    out -= in;
    break;
  case StatsAgg::ASSIGN:
    out = in;
    break;
  }
}

static void aggregateStat(StatsAgg agg, StatsAggOptional override,
                          StatsCounter& out, int64_t in) {
  aggregateStat(override.has_value() ? override.value() : agg, out, in);
}

// ----------------------------------------------------------------------------

void PerStreamStats::aggregate(PerStreamStats const& other,
                               StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg)                                                 \
  aggregateStat(StatsAgg::agg, agg_override, name, other.name);
#include "per_stream_stats.inc"
}

folly::dynamic PerStreamStats::toJsonObj() {
  folly::dynamic map = folly::dynamic::object;
#define STAT_DEFINE(name, _)                                                   \
  /* we know that all names are unique */                                      \
  map[#name] = name.load();
#include "per_stream_stats.inc"
  return map;
}

std::string PerStreamStats::toJson() {
  return folly::toJson(this->toJsonObj());
}

// ----------------------------------------------------------------------------
// All Stats

PerStreamTimeSeries::Duration
StatsParams::maxInterval(std::string string_name) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == string_name) {                                                  \
      return this->time_intervals_##name.back();                               \
    }                                                                          \
  }
#include "per_stream_time_series.inc"
  ld_check(false);
  return PerStreamTimeSeries::Duration{};
}

Stats::Stats(const FastUpdateableSharedPtr<StatsParams>* params)
    : params(params) {}

Stats::~Stats() = default;

Stats::Stats(const Stats& other) : Stats(other.params) {
  aggregate(other, StatsAgg::ASSIGN);
}

Stats& Stats::operator=(const Stats& other) {
  aggregate(other, StatsAgg::ASSIGN);
  return *this;
}

Stats::Stats(Stats&& other) noexcept(true) = default;

Stats& Stats::operator=(Stats&& other) noexcept(false) = default;

void Stats::aggregate(Stats const& other, StatsAggOptional agg_override) {
  aggregateCompoundStats(other, agg_override);
}

void Stats::aggregateForDestroyedThread(Stats const& other) {
  aggregateCompoundStats(other, folly::none, true);
}

void Stats::aggregateCompoundStats(Stats const& other,
                                   StatsAggOptional agg_override,
                                   bool destroyed_threads) {

  // Aggregate per stream stats. Use synchronizedCopy() to copy other's
  // per_stream_stats into temporary vector, to avoid holding a read lock on it
  // while we aggregate.
  this->per_stream_stats.withWLock(
      [&agg_override, other_per_stream_stats_entries =
                          other.synchronizedCopy(&Stats::per_stream_stats)](
          auto& this_per_stream_stats) {
        for (const auto& kv : other_per_stream_stats_entries) {
          ld_check(kv.second != nullptr);
          auto& stats_ptr = this_per_stream_stats[kv.first];
          if (stats_ptr == nullptr) {
            stats_ptr = std::make_shared<PerStreamStats>();
          }
          stats_ptr->aggregate(*kv.second, agg_override);
        }
      });
}

void Stats::deriveStats() {}

void Stats::reset() { per_stream_stats.wlock()->clear(); }

folly::dynamic Stats::toJsonObj() {
  folly::dynamic result = folly::dynamic::object;

  // per_stream_stats
  folly::dynamic per_stream_stats_obj = folly::dynamic::object;
  auto per_stream_stats = this->synchronizedCopy(&Stats::per_stream_stats);
  for (const auto& s : per_stream_stats) {
    ld_check(s.second != nullptr);
    per_stream_stats_obj[s.first] = s.second->toJsonObj();
  }
  result["per_stream_stats"] = per_stream_stats_obj;

  return result;
}
std::string Stats::toJson() { return folly::toJson(this->toJsonObj()); }

// ----------------------------------------------------------------------------

StatsHolder::StatsHolder(StatsParams params)
    : params_(std::make_shared<StatsParams>(std::move(params))),
      dead_stats_(&params_) {}

Stats* StatsHolder::aggregate() const {
  Stats* result = new Stats(&params_);

  {
    auto accessor = thread_stats_.accessAllThreads();
    result->aggregate(dead_stats_);
    for (const auto& x : accessor) {
      result->aggregate(x.stats);
    }
  }

  result->deriveStats();

  return result;
}

void StatsHolder::reset() {
  auto accessor = thread_stats_.accessAllThreads();
  dead_stats_.reset();
  for (auto& x : accessor) {
    x.stats.reset();
  }
}

void StatsHolder::print() {
  auto accessor = thread_stats_.accessAllThreads();
  std::cout << "* DeadThreads: " << dead_stats_.toJson() << "\n";
  for (auto it = accessor.begin(); it != accessor.end(); ++it) {
    auto& x = *it;
    std::cout << "* Thread " << it.getThreadId() << ": " << x.stats.toJson()
              << "\n";
  }
  std::cout.flush();
}

StatsHolder::~StatsHolder() {
  // No need to update dead_stats_ when StatsHolder is being destroyed.
  for (auto& x : thread_stats_.accessAllThreads()) {
    x.owner = nullptr;
  }
}
