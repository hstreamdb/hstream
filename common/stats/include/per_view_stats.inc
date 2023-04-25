#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of queries execute in view
STAT_DEFINE(total_execute_queries, SUM)

#undef STAT_DEFINE

