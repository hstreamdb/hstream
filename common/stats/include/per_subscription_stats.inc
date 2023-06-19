/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

STAT_DEFINE(send_out_bytes, SUM)
STAT_DEFINE(send_out_records, SUM)
// Total number of failed sends, exclude resends
STAT_DEFINE(send_out_records_failed, SUM)

// Total number of success resend records
STAT_DEFINE(resend_records, SUM)
// Total number of failed resend records 
STAT_DEFINE(resend_records_failed, SUM)

STAT_DEFINE(received_acks, SUM)

STAT_DEFINE(request_messages, SUM)
STAT_DEFINE(response_messages, SUM)

// ========== Internal Stats =================
STAT_DEFINE(checklist_size, SUM)

#undef STAT_DEFINE
