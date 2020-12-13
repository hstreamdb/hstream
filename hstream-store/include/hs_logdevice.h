#include <HsFFI.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <logdevice/include/types.h>

#ifndef HS_LOGDEVICE
#define HS_LOGDEVICE

#ifdef __cplusplus
extern "C" {
#endif

typedef struct logdevice_client_t logdevice_client_t;
typedef struct logdevice_reader_t logdevice_reader_t;

typedef struct logdevice_data_record_t {
  uint64_t logid;
  uint64_t lsn;
  char *payload;
  size_t payload_len;
} logdevice_data_record_t;

// LogID
typedef uint64_t c_logid_t;
extern const c_logid_t C_LOGID_INVALID;
extern const c_logid_t C_LOGID_INVALID2;
extern const c_logid_t C_LOGID_MAX;
extern const c_logid_t C_USER_LOGID_MAX;
extern const size_t C_LOGID_MAX_BITS;

// LogSequenceNumber
typedef uint64_t c_lsn_t;
const c_lsn_t C_LSN_INVALID = facebook::logdevice::LSN_INVALID;
const c_lsn_t C_LSN_OLDEST = facebook::logdevice::LSN_OLDEST;
const c_lsn_t C_LSN_MAX = facebook::logdevice::LSN_MAX;

/* ---------------------------------- */

void set_dbg_level_error(void);
void init_logdevice(void);

logdevice_client_t *new_logdevice_client(char *config_path);
void free_logdevice_client(logdevice_client_t *client);

logdevice_reader_t *new_logdevice_reader(logdevice_client_t *client,
                                         size_t max_logs, ssize_t buffer_size);
void free_logdevice_reader(logdevice_reader_t *reader);

c_lsn_t logdevice_get_tail_lsn_sync(logdevice_client_t *client, uint64_t logid);

c_lsn_t logdevice_append_sync(logdevice_client_t *client, uint64_t logid,
                              const char *payload, HsInt offset, HsInt length,
                              int64_t *ts);

int logdevice_reader_start_reading(logdevice_reader_t *reader, c_logid_t logid,
                                   c_lsn_t start, c_lsn_t until);
bool logdevice_reader_is_reading(logdevice_reader_t *reader, c_logid_t logid);
bool logdevice_reader_is_reading_any(logdevice_reader_t *reader);
int logdevice_reader_read_sync(logdevice_reader_t *reader, size_t maxlen,
                               logdevice_data_record_t *data_out,
                               ssize_t *len_out);

#ifdef __cplusplus
} /* end extern "C" */
#endif

// End define HS_LOGDEVICE
#endif
