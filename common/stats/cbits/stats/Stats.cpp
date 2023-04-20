#include "Stats.h"

#include <folly/String.h>
#include <iostream>

namespace hstream { namespace common {
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

template <typename H>
static void aggregateHistogram(StatsAggOptional agg, H& out, const H& in) {
  if (!agg.hasValue()) {
    out.merge(in);
    return;
  }
  switch (agg.value()) {
  case StatsAgg::SUM:
    out.merge(in);
    break;
  case StatsAgg::MAX:
    // MAX doesn't make much sense for histograms. Let's just merge them.
    out.merge(in);
    break;
  case StatsAgg::SUBTRACT:
    out.subtract(in);
    break;
  case StatsAgg::ASSIGN:
    out = in;
    break;
  }
}

// ----------------------------------------------------------------------------

void PerStreamStats::aggregate(PerStreamStats const& other,
                               StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg)                                                 \
  aggregateStat(StatsAgg::agg, agg_override, name##_counter,                   \
                other.name##_counter);
#include "per_stream_stats.inc"
}

folly::dynamic PerStreamStats::toJsonObj() {
  folly::dynamic map = folly::dynamic::object;
#define STAT_DEFINE(name, _)                                                   \
  /* we know that all names are unique */                                      \
  map[#name] = name##_counter.load();
#include "per_stream_stats.inc"
  return map;
}

std::string PerStreamStats::toJson() {
  return folly::toJson(this->toJsonObj());
}

void PerConnectorStats::aggregate(PerConnectorStats const& other,
                               StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg)                                                 \
  aggregateStat(StatsAgg::agg, agg_override, name##_counter,                   \
                other.name##_counter);
#include "per_connector_stats.inc"
}

folly::dynamic PerConnectorStats::toJsonObj() {
  folly::dynamic map = folly::dynamic::object;
#define STAT_DEFINE(name, _)                                                   \
  /* we know that all names are unique */                                      \
  map[#name] = name##_counter.load();
#include "per_connector_stats.inc"
  return map;
}

std::string PerConnectorStats::toJson() {
  return folly::toJson(this->toJsonObj());
}

void PerQueryStats::aggregate(PerQueryStats const& other,
                              StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg)                                                 \
  aggregateStat(StatsAgg::agg, agg_override, name##_counter,                   \
                other.name##_counter);
#include "per_query_stats.inc"
}

folly::dynamic PerQueryStats::toJsonObj() {
  folly::dynamic map = folly::dynamic::object;
#define STAT_DEFINE(name, _)                                                   \
  /* we know that all names are unique */                                      \
  map[#name] = name##_counter.load();
#include "per_query_stats.inc"
  return map;
}

std::string PerQueryStats::toJson() {
  return folly::toJson(this->toJsonObj());
}

void PerSubscriptionStats::aggregate(PerSubscriptionStats const& other,
                                     StatsAggOptional agg_override) {
#define STAT_DEFINE(name, agg)                                                 \
  aggregateStat(StatsAgg::agg, agg_override, name##_counter,                   \
                other.name##_counter);
#include "per_subscription_stats.inc"
}

folly::dynamic PerSubscriptionStats::toJsonObj() {
  folly::dynamic map = folly::dynamic::object;
#define STAT_DEFINE(name, _)                                                   \
  /* we know that all names are unique */                                      \
  map[#name] = name##_counter.load();
#include "per_subscription_stats.inc"
  return map;
}

std::string PerSubscriptionStats::toJson() {
  return folly::toJson(this->toJsonObj());
}

// ----------------------------------------------------------------------------

PerStreamTimeSeries::Duration
StatsParams::maxStreamStatsInterval(std::string string_name) {
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

PerStreamTimeSeries::Duration
StatsParams::maxSubscribptionStatsInterval(std::string string_name) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == string_name) {                                                  \
      return this->time_intervals_##name.back();                               \
    }                                                                          \
  }
#include "per_subscription_time_series.inc"
  ld_check(false);
  return PerStreamTimeSeries::Duration{};
}

PerHandleTimeSeries::Duration
StatsParams::maxHandleStatsInterval(std::string string_name) {
#define TIME_SERIES_DEFINE(name, strings, _, __)                               \
  for (const std::string& str : strings) {                                     \
    if (str == string_name) {                                                  \
      return this->time_intervals_##name.back();                               \
    }                                                                          \
  }
#include "per_handle_time_series.inc"
  ld_check(false);
  return PerStreamTimeSeries::Duration{};
}

// ----------------------------------------------------------------------------
// All Stats

Stats::Stats(const FastUpdateableSharedPtr<StatsParams>* params)
    : params(params) {
  if (params->get()->is_server) {
    server_histograms = std::make_unique<ServerHistograms>();
  }
}

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
#define _PER_STATS(x, ty)                                                      \
  this->x.withWLock([&agg_override, other_stats = other.synchronizedCopy(      \
                                        &Stats::x)](auto& this_stats) {        \
    for (const auto& kv : other_stats) {                                       \
      ld_check(kv.second != nullptr);                                          \
      auto& stats_ptr = this_stats[kv.first];                                  \
      if (stats_ptr == nullptr) {                                              \
        stats_ptr = std::make_shared<ty>();                                    \
      }                                                                        \
      stats_ptr->aggregate(*kv.second, agg_override);                          \
    }                                                                          \
  });

  // Aggregate per stream stats. Use synchronizedCopy() to copy other's
  // per_stream_stats into temporary vector, to avoid holding a read lock on it
  // while we aggregate.
  _PER_STATS(per_stream_stats, PerStreamStats)
  _PER_STATS(per_connector_stats, PerConnectorStats)
  _PER_STATS(per_query_stats, PerQueryStats)
  _PER_STATS(per_subscription_stats, PerSubscriptionStats)
#undef _PER_STATS

  if (other.server_histograms) {
    ld_check(server_histograms);
    aggregateHistogram(agg_override, *server_histograms,
                       *other.server_histograms);
  }
}

void Stats::deriveStats() {}

void Stats::reset() {
  per_stream_stats.wlock()->clear();
  per_connector_stats.wlock()->clear();
  per_query_stats.wlock()->clear();
  per_subscription_stats.wlock()->clear();
  per_handle_stats.wlock()->clear();

  if (params->get()->is_server) {
    if (server_histograms) {
      server_histograms->clear();
    }
  }
}

folly::dynamic Stats::toJsonObj() {
  folly::dynamic result = folly::dynamic::object;

#define _PER_TO_JSON(x)                                                        \
  folly::dynamic x##_obj = folly::dynamic::object;                             \
  auto x##_stats = this->synchronizedCopy(&Stats::x);                          \
  for (const auto& s : x##_stats) {                                            \
    ld_check(s.second != nullptr);                                             \
    x##_obj[s.first] = s.second->toJsonObj();                                  \
  }                                                                            \
  result[#x] = x##_obj;

  _PER_TO_JSON(per_stream_stats)
  _PER_TO_JSON(per_connector_stats)
  _PER_TO_JSON(per_query_stats)
  _PER_TO_JSON(per_subscription_stats)
#undef _PER_TO_JSON

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

Stats StatsHolder::aggregate_nonew() const {
  Stats result = Stats(&params_);

  {
    auto accessor = thread_stats_.accessAllThreads();
    result.aggregate(dead_stats_);
    for (const auto& x : accessor) {
      result.aggregate(x.stats);
    }
  }

  result.deriveStats();

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

}} // namespace hstream::common
