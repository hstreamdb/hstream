/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Number of consumers 
STAT_DEFINE(consumers, SUM)

#undef STAT_DEFINE
