#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of append requests
STAT_DEFINE(cs_append_total, SUM)
// Failed append requests
STAT_DEFINE(cs_append_failed, SUM)

// Total payload bytes successfully written to the cache store
STAT_DEFINE(cs_append_in_bytes, SUM)
// Total payload records successfully written to the cache store
STAT_DEFINE(cs_append_in_records, SUM)

// ============== read stats =========================
// Total payload bytes successfully read from the cache store
STAT_DEFINE(cs_read_in_bytes, SUM)
// Total payload records successfully read from the cache store
STAT_DEFINE(cs_read_in_records, SUM)

// Total number of delivered records
STAT_DEFINE(cs_delivered_in_records, SUM)
STAT_DEFINE(cs_delivered_total, SUM)
STAT_DEFINE(cs_delivered_failed, SUM)

#undef STAT_DEFINE
