#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Payload bytes sent by clients in APPENDs
STAT_DEFINE(append_payload_bytes, SUM)

// Payload bytes sent to clients in RECORDs
STAT_DEFINE(record_payload_bytes, SUM)

#undef STAT_DEFINE
