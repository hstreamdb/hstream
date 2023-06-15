#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total items of subscription checklist
STAT_DEFINE(sub_checklist_size, SUM)

#undef STAT_DEFINE
