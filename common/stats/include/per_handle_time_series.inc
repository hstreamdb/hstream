#ifndef TIME_SERIES_DEFINE
#error TIME_SERIES_DEFINE() macro not defined
#define TIME_SERIES_DEFINE(...)
#endif

// Fields:
// 1) name,
// 2) string names for querying via admin command, should contain
//    one same as the name at least.
// 3) time intervals over which to accumulate the stats
// 4) number of buckets for time series

// The choice of intervals and buckets per interval depends on the required
// granularity of reporting (expected queries) and desired memory usage.
//
// E.g., with {10, 60, 600} seconds and 2 buckets we have:
// - 5-second granularity at the 10-second level,
// - 30-second granularity at the 1-minute level, and
// - 5-minute granularity at the 10-minute level
// ... at a cost of 3 * 2 * 16B = 96B per (active) handle per worker.

TIME_SERIES_DEFINE(queries_in,
                   std::set<std::string>({"queries_in", "queries"}),
                   std::vector<std::chrono::milliseconds>({
                       std::chrono::seconds(4),
                       std::chrono::seconds(60),
                       std::chrono::seconds(300),
                       std::chrono::seconds(600)}),
                   2)

#undef TIME_SERIES_DEFINE
