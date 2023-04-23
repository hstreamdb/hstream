#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of records read from source
STAT_DEFINE(total_input_records, SUM)
// Total number of records write to sink
STAT_DEFINE(total_output_records, SUM)

#undef STAT_DEFINE

