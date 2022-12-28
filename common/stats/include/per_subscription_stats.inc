/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Total number of all resend records
STAT_DEFINE(delivery_in_bytes, SUM)
STAT_DEFINE(delivery_in_records, SUM)
STAT_DEFINE(received_acks, SUM)
STAT_DEFINE(resend_records, SUM)
STAT_DEFINE(request_messages_counter, SUM)
STAT_DEFINE(response_messages_counter, SUM)

#undef STAT_DEFINE
