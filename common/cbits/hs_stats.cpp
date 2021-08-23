#include "hs_common.h"
#include <stats.h>

extern "C" {
// ----------------------------------------------------------------------------

Stats* new_stats() { return new Stats; }
void delete_stats(Stats* s) { delete s; }

StatsHolder* new_stats_holder() { return new StatsHolder; }
void delete_stats_holder(StatsHolder* s) { delete s; }

void stats_holder_print(StatsHolder* s) { s->print(); }

// ----------------------------------------------------------------------------
// PerStreamStats

#define STAT_DEFINE(name, _)                                                   \
  void stream_stat_add_##name(StatsHolder* stats_holder, Stats* stats_struct,  \
                              const char* stream_name, int64_t val) {          \
    if (stats_holder)                                                          \
      stats_struct = &(stats_holder->get());                                   \
    STREAM_STAT_ADD(stats_struct, stream_name, name, val);                     \
  }                                                                            \
  int64_t stream_stat_get_##name(StatsHolder* stats_holder,                    \
                                 Stats* stats_struct,                          \
                                 const char* stream_name) {                    \
    if (stats_holder)                                                          \
      stats_struct = &(stats_holder->get());                                   \
    auto stats_ulock = (stats_struct)->per_stream_stats.ulock();               \
    auto stats_it = stats_ulock->find(std::string(stream_name));               \
    if (stats_it != stats_ulock->end()) {                                      \
      return stats_it->second->name.load();                                    \
    } else {                                                                   \
      return -1;                                                               \
    }                                                                          \
  }                                                                            \
  int64_t stream_stat_getall_##name(StatsHolder* stats_holder,                 \
                                    const char* stream_name) {                 \
    auto stats_struct = stats_holder->aggregate();                             \
    auto stats_ulock = (stats_struct).per_stream_stats.ulock();                \
    auto stats_it = stats_ulock->find(std::string(stream_name));               \
    if (stats_it != stats_ulock->end()) {                                      \
      return stats_it->second->name.load();                                    \
    } else {                                                                   \
      return -1;                                                               \
    }                                                                          \
  }
#include "per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
  void stream_time_series_add_##name(Stats* stats_struct,                      \
                                     const char* stream_name, int64_t val) {   \
    STREAM_TIME_SERIES_ADD(stats_struct, stream_name, name, val);              \
  }                                                                            \
  void stream_time_series_flush_##name(Stats* stats_struct,                    \
                                       const char* stream_name) {              \
    auto stats_ulock = (stats_struct)->per_stream_stats.ulock();               \
    auto stats_it = stats_ulock->find(std::string(stream_name));               \
    if (UNLIKELY(stats_it == stats_ulock->end())) {                            \
      /* TODO: error */                                                        \
    } else                                                                     \
      return stats_it->second->name->flush();                                  \
  }                                                                            \
  double stream_time_series_get_rate_##name(                                   \
      Stats* stats_struct, const char* stream_name, size_t level) {            \
    auto stats_ulock = (stats_struct)->per_stream_stats.ulock();               \
    auto stats_it = stats_ulock->find(std::string(stream_name));               \
    if (stats_it != stats_ulock->end()) {                                      \
      auto& stat_ts = stats_it->second->name;                                  \
      return stats_it->second->name->rate(level);                              \
    } else {                                                                   \
      return -1;                                                               \
    }                                                                          \
  }
#include "per_stream_time_series.inc"

// ----------------------------------------------------------------------------
}
