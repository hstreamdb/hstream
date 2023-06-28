/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// ========== Internal Stats =================

STAT_DEFINE(checklist_size, SUM)

#undef STAT_DEFINE
