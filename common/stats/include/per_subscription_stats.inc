/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of all resend records
STAT_DEFINE(resend_records, SUM)

#undef STAT_DEFINE
