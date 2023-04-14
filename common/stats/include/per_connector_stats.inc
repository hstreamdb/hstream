#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of delivered records
STAT_DEFINE(delivered_in_records, SUM)
// Total number of delivered bytes
STAT_DEFINE(delivered_in_bytes, SUM)

#undef STAT_DEFINE
