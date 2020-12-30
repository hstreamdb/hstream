#include <HsFFI.h>
#include <iostream>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/ClientSettings.h>
#include <logdevice/include/Err.h>
#include <logdevice/include/LogAttributes.h>
#include <logdevice/include/LogsConfigTypes.h>
#include <logdevice/include/Reader.h>
#include <logdevice/include/Record.h>
#include <logdevice/include/RecordOffset.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>

#ifndef HS_LOGDEVICE
#define HS_LOGDEVICE

using facebook::logdevice::AppendAttributes;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::ClientSettings;
using facebook::logdevice::KeyType;
using facebook::logdevice::Reader;
using facebook::logdevice::client::LogAttributes;
using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
using facebook::logdevice::client::LogGroup;
using LogDirectory = facebook::logdevice::client::Directory;

#ifdef __cplusplus
extern "C" {
#endif

struct logdevice_loggroup_t {
  std::unique_ptr<LogGroup> rep;
};
struct logdevice_logdirectory_t {
  std::unique_ptr<LogDirectory> rep;
};
struct logdevice_client_t {
  std::shared_ptr<Client> rep;
};
struct logdevice_reader_t {
  std::unique_ptr<Reader> rep;
};
typedef struct logdevice_client_t logdevice_client_t;
typedef struct logdevice_reader_t logdevice_reader_t;
typedef struct logdevice_logdirectory_t logdevice_logdirectory_t;
typedef struct logdevice_loggroup_t logdevice_loggroup_t;

// LogID
typedef uint64_t c_logid_t;
const c_logid_t C_LOGID_INVALID = facebook::logdevice::LOGID_INVALID.val();
const c_logid_t C_LOGID_INVALID2 = facebook::logdevice::LOGID_INVALID2.val();
// max valid data logid value. This accounts for internal logs.
// Not to be confused with numeric_limits<>::max().
const c_logid_t C_LOGID_MAX = facebook::logdevice::LOGID_MAX.val();
// max valid user data logid value.
const c_logid_t C_USER_LOGID_MAX = facebook::logdevice::USER_LOGID_MAX.val();
// maximum number of bits in a log id
const size_t C_LOGID_MAX_BITS = facebook::logdevice::LOGID_BITS;

// LogAttributes
LogAttributes* new_log_attributes();
void free_log_attributes(LogAttributes* attrs);
void with_replicationFactor(LogAttributes* attrs, int value);

// LogSequenceNumber
typedef uint64_t c_lsn_t;
const c_lsn_t C_LSN_INVALID = facebook::logdevice::LSN_INVALID;
const c_lsn_t C_LSN_OLDEST = facebook::logdevice::LSN_OLDEST;
const c_lsn_t C_LSN_MAX = facebook::logdevice::LSN_MAX;

// DataRecord
typedef struct logdevice_data_record_t {
  uint64_t logid;
  uint64_t lsn;
  char* payload;
  size_t payload_len;
} logdevice_data_record_t;

// LogGroup
void ld_loggroup_get_range(logdevice_loggroup_t* group, c_logid_t* start,
                           c_logid_t* end);

const char* ld_loggroup_get_name(logdevice_loggroup_t* group);

// KeyType
typedef uint8_t c_keytype_t;
const c_keytype_t C_KeyType_FINDKEY = static_cast<c_keytype_t>(KeyType::FINDKEY);
const c_keytype_t C_KeyType_FILTERABLE = static_cast<c_keytype_t>(KeyType::FILTERABLE);
const c_keytype_t C_KeyType_MAX = static_cast<c_keytype_t>(KeyType::MAX);
const c_keytype_t C_KeyType_UNDEFINED = static_cast<c_keytype_t>(KeyType::UNDEFINED);

// Err
const char* show_error_name(facebook::logdevice::E err);
const char* show_error_description(facebook::logdevice::E err);

// ----------------------------------------------------------------------------

// Create & Free Client
facebook::logdevice::Status
new_logdevice_client(char* config_path, logdevice_client_t** client_ret);

void free_logdevice_client(logdevice_client_t* client);

// Create & Free Reader
logdevice_reader_t* new_logdevice_reader(logdevice_client_t* client,
                                         size_t max_logs, ssize_t buffer_size);
void free_logdevice_reader(logdevice_reader_t* reader);

// ----------------------------------------------------------------------------
// Client

size_t ld_client_get_max_payload_size(logdevice_client_t* client);

c_lsn_t ld_client_get_tail_lsn_sync(logdevice_client_t* client, uint64_t logid);

const std::string* ld_client_get_settings(logdevice_client_t* client,
                                          const char* name);

facebook::logdevice::Status ld_client_set_settings(logdevice_client_t* client,
                                                   const char* name,
                                                   const char* value);

// ----------------------------------------------------------------------------
// LogConfigType: LogDirectory

facebook::logdevice::Status
ld_client_make_directory_sync(logdevice_client_t* client, const char* path,
                              bool mk_intermediate_dirs, LogAttributes* attrs,
                              logdevice_logdirectory_t** logdir_ret);

void* free_lodevice_logdirectory(logdevice_logdirectory_t* dir);

const char* lg_logdirectory_get_name(logdevice_logdirectory_t* dir);

// ----------------------------------------------------------------------------

facebook::logdevice::Status ld_client_make_loggroup_sync(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    logdevice_loggroup_t** loggroup_result);

facebook::logdevice::Status
ld_client_get_loggroup_sync(logdevice_client_t* client, const char* path,
                            logdevice_loggroup_t** loggroup_result);

void* free_lodevice_loggroup(logdevice_loggroup_t* group);

// ----------------------------------------------------------------------------

facebook::logdevice::Status
logdevice_append_sync(logdevice_client_t* client, c_logid_t logid,
                      const char* payload, HsInt offset,
                      HsInt length, // payload
                      int64_t* ts, c_lsn_t* lsn_ret);

facebook::logdevice::Status
logdevice_append_with_attrs_sync(logdevice_client_t* client, c_logid_t logid,
                                 const char* payload, HsInt offset,
                                 HsInt length, // payload
                                 KeyType keytype,
                                 const char* keyval, // optional_key
                                 int64_t* ts, c_lsn_t* lsn_ret);

int logdevice_reader_start_reading(logdevice_reader_t* reader, c_logid_t logid,
                                   c_lsn_t start, c_lsn_t until);
bool logdevice_reader_is_reading(logdevice_reader_t* reader, c_logid_t logid);
bool logdevice_reader_is_reading_any(logdevice_reader_t* reader);
int logdevice_reader_read_sync(logdevice_reader_t* reader, size_t maxlen,
                               logdevice_data_record_t* data_out,
                               ssize_t* len_out);

// ----------------------------------------------------------------------------
// Misc

void set_dbg_level_error(void);

#ifdef __cplusplus
} /* end extern "C" */
#endif

// End define HS_LOGDEVICE
#endif
