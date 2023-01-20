/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

STAT_DEFINE(delivery_in_bytes, SUM)
STAT_DEFINE(delivery_in_records, SUM)
// Total number of failed delivery, exclude resend
STAT_DEFINE(delivery_failed_records, SUM)

STAT_DEFINE(received_acks, SUM)

// Total number of success resend records
STAT_DEFINE(resend_in_records, SUM)
// Total number of failed resend records 
STAT_DEFINE(resend_failed_records, SUM)

STAT_DEFINE(request_messages_counter, SUM)
STAT_DEFINE(response_messages_counter, SUM)

#undef STAT_DEFINE
