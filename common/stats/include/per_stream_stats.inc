#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Payload bytes sent by clients in APPENDs
STAT_DEFINE(append_payload_bytes, SUM)

// Payload bytes sent to clients in RECORDs
STAT_DEFINE(record_payload_bytes, SUM)

// Total number of append requests
STAT_DEFINE(append_total, SUM)
// Failed append requests
STAT_DEFINE(append_failed, SUM)

#undef STAT_DEFINE
