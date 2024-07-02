#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of append requests
STAT_DEFINE(append_total, SUM)
// Failed append requests
STAT_DEFINE(append_failed, SUM)

// Total payload bytes successfully written to the cache store
STAT_DEFINE(append_in_bytes, SUM)
// Total payload records successfully written to the cache store
STAT_DEFINE(append_in_records, SUM)

// ============== read stats =========================
// Total payload bytes successfully read from the cache store
STAT_DEFINE(read_in_bytes, SUM)
// Total payload records successfully read from the cache store
STAT_DEFINE(read_in_records, SUM)

// Total number of delivered records
STAT_DEFINE(delivered_in_records, SUM)

#undef STAT_DEFINE
