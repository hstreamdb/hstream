#pragma once

// * --------------------------------------------------------------------------

#include "ghc_ext.h"
#include "hs_cpp_lib.h"

#include <iostream>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <unordered_map>

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <folly/String.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>

#include <logdevice/common/PayloadHolder.h>
#include <logdevice/common/Processor.h>
#include <logdevice/common/RSMBasedVersionedConfigStore.h>
#include <logdevice/common/VersionedConfigStore.h>
#include <logdevice/common/buffered_writer/BufferedWriteCodec.h>
#include <logdevice/common/checks.h>
#include <logdevice/common/debug.h>
#include <logdevice/common/types_internal.h>
#include <logdevice/include/BufferedWriter.h>
#include <logdevice/include/CheckpointStore.h>
#include <logdevice/include/CheckpointStoreFactory.h>
#include <logdevice/include/CheckpointedReaderBase.h>
#include <logdevice/include/CheckpointedReaderFactory.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/ClientSettings.h>
#include <logdevice/include/Err.h>
#include <logdevice/include/LogAttributes.h>
#include <logdevice/include/LogHeadAttributes.h>
#include <logdevice/include/LogTailAttributes.h>
#include <logdevice/include/LogsConfigTypes.h>
#include <logdevice/include/Reader.h>
#include <logdevice/include/Record.h>
#include <logdevice/include/RecordOffset.h>
#include <logdevice/include/SyncCheckpointedReader.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>
#include <logdevice/lib/ClientImpl.h>

namespace ld = facebook::logdevice;
using facebook::logdevice::AppendAttributes;
using facebook::logdevice::BufferedWriteCodec;
using facebook::logdevice::BufferedWriteSinglePayloadsCodec;
using facebook::logdevice::CheckpointedReaderBase;
using facebook::logdevice::CheckpointedReaderFactory;
using facebook::logdevice::CheckpointStore;
using facebook::logdevice::CheckpointStoreFactory;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::ClientImpl;
using facebook::logdevice::ClientSettings;
using facebook::logdevice::Compression;
using facebook::logdevice::DataRecord;
using facebook::logdevice::KeyType;
using facebook::logdevice::LogHeadAttributes;
using facebook::logdevice::logid_t;
using facebook::logdevice::LogTailAttributes;
using facebook::logdevice::lsn_t;
using facebook::logdevice::Payload;
using facebook::logdevice::PayloadHolder;
using facebook::logdevice::Processor;
using facebook::logdevice::Reader;
using facebook::logdevice::RecordOffset;
using facebook::logdevice::RSMBasedVersionedConfigStore;
using facebook::logdevice::SharedCheckpointedReaderBase;
using facebook::logdevice::SharedSyncCheckpointedReader;
using facebook::logdevice::SyncCheckpointedReader;
using facebook::logdevice::vcs_config_version_t;
using facebook::logdevice::VersionedConfigStore;
using facebook::logdevice::worker_id_t;
using facebook::logdevice::client::LogAttributes;
using facebook::logdevice::client::LogGroup;
using LogDirectory = facebook::logdevice::client::Directory;

std::string* new_hs_std_string(std::string&& str);
char* copyString(const std::string& str);

template <typename Container>
std::vector<std::string>* getKeys(const Container& container) {
  std::vector<std::string>* keys = new std::vector<std::string>;
  for (const auto& x : container) {
    keys->push_back(x.first);
  }
  return keys;
}

// ----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif

std::string* hs_cal_std_string_off(std::string* str, HsInt idx);

// ----------------------------------------------------------------------------

void init_logdevice(void);

typedef int64_t c_timestamp_t;
const c_timestamp_t MAX_MILLISECONDS = std::chrono::milliseconds::max().count();

typedef uint16_t c_error_code_t;
typedef uint8_t c_keytype_t;
typedef uint8_t c_compression_t;
typedef uint64_t c_vcs_config_version_t;

struct logdevice_loggroup_t {
  std::unique_ptr<LogGroup> rep;
};
struct logdevice_logdirectory_t {
  std::unique_ptr<LogDirectory> rep;
};
struct logdevice_client_t {
  std::shared_ptr<Client> rep;
};
struct logdevice_vcs_t {
  std::unique_ptr<VersionedConfigStore> rep;
};

struct logdevice_log_head_attributes_t {
  std::unique_ptr<LogHeadAttributes> rep;
};

typedef struct logdevice_reader_t {
  std::unique_ptr<Reader> rep;
} logdevice_reader_t;

#define HSTREAM_USE_SHARED_CHECKPOINT_STORE

#ifdef HSTREAM_USE_SHARED_CHECKPOINT_STORE

typedef struct logdevice_checkpoint_store_t {
  std::shared_ptr<CheckpointStore> rep;
} logdevice_checkpoint_store_t;

typedef struct logdevice_sync_checkpointed_reader_t {
  std::unique_ptr<SharedSyncCheckpointedReader> rep;
} logdevice_sync_checkpointed_reader_t;

#else

typedef struct logdevice_checkpoint_store_t {
  std::unique_ptr<CheckpointStore> rep;
} logdevice_checkpoint_store_t;

typedef struct logdevice_sync_checkpointed_reader_t {
  std::unique_ptr<SyncCheckpointedReader> rep;
} logdevice_sync_checkpointed_reader_t;

#endif

#undef HSTREAM_USE_SHARED_CHECKPOINT_STORE

typedef struct logdevice_loggroup_t logdevice_loggroup_t;
typedef struct logdevice_logdirectory_t logdevice_logdirectory_t;
typedef struct logdevice_client_t logdevice_client_t;
typedef struct logdevice_vcs_t logdevice_vcs_t;
typedef struct logdevice_log_head_attributes_t logdevice_log_head_attributes_t;
typedef struct log_tail_attributes_cb_data_t log_tail_attributes_cb_data_t;

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
#if __GLASGOW_HASKELL__ < 810
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgMutArrPtrs* keys_, StgMutArrPtrs* vals_);
#else
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgArrBytes** keys, StgArrBytes** vals);
#endif
void free_log_attributes(LogAttributes* attrs);
bool exist_log_attrs_extras(LogAttributes* attrs, char* key);
std::string* get_log_attrs_extra(LogAttributes* attrs, char* key);
void get_attribute_extras(LogAttributes* attrs, size_t* len,
                          std::string** keys_ptr, std::string** values_ptr,
                          std::vector<std::string>** keys_,
                          std::vector<std::string>** values_);

// LogSequenceNumber
typedef uint64_t c_lsn_t;
const c_lsn_t C_LSN_INVALID = facebook::logdevice::LSN_INVALID;
const c_lsn_t C_LSN_OLDEST = facebook::logdevice::LSN_OLDEST;
const c_lsn_t C_LSN_MAX = facebook::logdevice::LSN_MAX;

// ----------------------------------------------------------------------------
// DataRecord

typedef struct logdevice_data_record_t {
  c_logid_t logid;
  // log sequence number (LSN) of this record
  c_lsn_t lsn;
  // timestamp in milliseconds since epoch assigned to this record by a
  // LogDevice server
  c_timestamp_t timestamp;
  // If the record is part of a batch written through BufferedWriter, this
  // contains the record's 0-based index within the batch.  (All records that
  // get batched into a single write by BufferedWriter have the same LSN.)
  HsInt batch_offset;
  // Payload
  char* payload;
  size_t payload_len;
  // RecordOffset object containing information on the amount of data written
  // to the log (to which this record belongs) up to this record.
  // Currently supports BYTE_OFFSET which represents the number of bytes
  // written. if counter is invalid it will be set as BYTE_OFFSET_INVALID.
  // BYTE_OFFSET will be invalid if this attribute was not requested by client
  // (see includeByteOffset() reader option) or if it is not available to
  // storage nodes.
  uint64_t byte_offset;
} logdevice_data_record_t;

typedef uint8_t c_gaptype_t;

typedef struct logdevice_gap_record_t {
  c_logid_t logid;
  c_gaptype_t gaptype;
  c_lsn_t lo;
  c_lsn_t hi;
} logdevice_gap_record_t;

// ----------------------------------------------------------------------------
// LogTailAttributes
typedef struct logdevice_log_tail_attributes_t {
  c_lsn_t last_released_real_lsn;
  c_timestamp_t last_timestamp;
  // RecordOffset struct contains information on amount of data written to
  // the log, but currently it only supports BYTE_OFFSET. So here the offsets
  // filed in the struct stores the amount of data in bytes written to the
  // log.
  uint64_t offsets;
} logdevice_log_tail_attributes_t;

bool valid_log_tail_attributes(logdevice_log_tail_attributes_t* tail_attr);

facebook::logdevice::Status
ld_client_get_tail_attributes(logdevice_client_t* client, c_logid_t logid,
                              HsStablePtr mvar, HsInt cap,
                              log_tail_attributes_cb_data_t* cb_data);
lsn_t ld_client_get_tail_attributes_lsn(
    logdevice_log_tail_attributes_t* tail_attr);
c_timestamp_t ld_client_get_tail_attributes_last_timestamp(
    logdevice_log_tail_attributes_t* tail_attr);
uint64_t ld_client_get_tail_attributes_bytes_offset(
    logdevice_log_tail_attributes_t* tail_attr);
void free_logdevice_tail_attributes(logdevice_log_tail_attributes_t* tail_attr);

// ----------------------------------------------------------------------------
// KeyType
const c_keytype_t C_KeyType_FINDKEY =
    static_cast<c_keytype_t>(KeyType::FINDKEY);
const c_keytype_t C_KeyType_FILTERABLE =
    static_cast<c_keytype_t>(KeyType::FILTERABLE);
const c_keytype_t C_KeyType_MAX = static_cast<c_keytype_t>(KeyType::MAX);
const c_keytype_t C_KeyType_UNDEFINED =
    static_cast<c_keytype_t>(KeyType::UNDEFINED);

// Used by VersionedConfigStore
typedef uint64_t c_vcs_config_version_t;

// Err
const char* show_error_name(facebook::logdevice::E err);
const char* show_error_description(facebook::logdevice::E err);

// Debug
typedef unsigned c_logdevice_dbg_level;
const c_logdevice_dbg_level C_DBG_CRITICAL = static_cast<c_logdevice_dbg_level>(
    facebook::logdevice::dbg::Level::CRITICAL);
const c_logdevice_dbg_level C_DBG_ERROR =
    static_cast<c_logdevice_dbg_level>(facebook::logdevice::dbg::Level::ERROR);
const c_logdevice_dbg_level C_DBG_WARNING = static_cast<c_logdevice_dbg_level>(
    facebook::logdevice::dbg::Level::WARNING);
const c_logdevice_dbg_level C_DBG_NOTIFY =
    static_cast<c_logdevice_dbg_level>(facebook::logdevice::dbg::Level::NOTIFY);
const c_logdevice_dbg_level C_DBG_INFO =
    static_cast<c_logdevice_dbg_level>(facebook::logdevice::dbg::Level::INFO);
const c_logdevice_dbg_level C_DBG_DEBUG =
    static_cast<c_logdevice_dbg_level>(facebook::logdevice::dbg::Level::DEBUG);
const c_logdevice_dbg_level C_DBG_SPEW =
    static_cast<c_logdevice_dbg_level>(facebook::logdevice::dbg::Level::SPEW);

void set_dbg_level(c_logdevice_dbg_level level);
int dbg_use_fd(int fd);

// ----------------------------------------------------------------------------
// callbacks

typedef struct logdevice_append_cb_data_t {
  c_error_code_t st;
  c_logid_t logid;
  c_lsn_t lsn;
  c_timestamp_t timestamp;
} logdevice_append_cb_data_t;

typedef struct make_directory_cb_data_t {
  c_error_code_t st;
  logdevice_logdirectory_t* directory;
  char* failure_reason;
} make_directory_cb_data_t;

typedef struct make_loggroup_cb_data_t {
  c_error_code_t st;
  logdevice_loggroup_t* loggroup;
  char* failure_reason;
} make_loggroup_cb_data_t;

typedef struct logsconfig_status_cb_data_t {
  c_error_code_t st;
  uint64_t version;
  char* failure_reason;
} logsconfig_status_cb_data_t;

// The status codes may be one of the following if the callback is invoked:
//   OK
//   NOTFOUND: key not found, corresponds to ZNONODE
//   VERSION_MISMATCH: corresponds to ZBADVERSION
//   ACCESS: permission denied, corresponds to ZNOAUTH
//   UPTODATE: current version is up-to-date for conditional get
//   AGAIN: transient errors (including connection closed ZCONNECTIONLOSS,
//          timed out ZOPERATIONTIMEOUT, throttled ZWRITETHROTTLE)
typedef struct vcs_value_callback_data_t {
  c_error_code_t st;
  HsInt val_len;
  const char* value;
} vcs_value_callback_data_t;
typedef struct vcs_write_callback_data_t {
  c_error_code_t st;
  c_vcs_config_version_t version;
  HsInt val_len;
  const char* value;
} vcs_write_callback_data_t;

typedef struct log_head_attributes_cb_data_t {
  c_error_code_t st;
  logdevice_log_head_attributes_t* head_attributes;
} log_head_attributes_cb_data_t;

typedef struct log_tail_attributes_cb_data_t {
  c_error_code_t st;
  logdevice_log_tail_attributes_t* tail_attributes;
} log_tail_attributes_cb_data_t;

typedef struct is_log_empty_cb_data_t {
  c_error_code_t st;
  bool empty;
} is_log_empty_cb_data_t;

// ----------------------------------------------------------------------------
// Client

facebook::logdevice::Status
new_logdevice_client(char* config_path, logdevice_client_t** client_ret);

void free_logdevice_client(logdevice_client_t* client);

size_t ld_client_get_max_payload_size(logdevice_client_t* client);

facebook::logdevice::Status
ld_client_is_log_empty(logdevice_client_t* client, c_logid_t logid,
                       HsStablePtr mvar, HsInt cap,
                       is_log_empty_cb_data_t* data);

c_lsn_t ld_client_get_tail_lsn_sync(logdevice_client_t* client, uint64_t logid);

HsInt ld_client_get_tail_lsn(logdevice_client_t* client, c_logid_t logid,
                             HsStablePtr mvar, HsInt cap,
                             c_error_code_t* st_out, c_lsn_t* lsn_out);

const std::string* ld_client_get_settings(logdevice_client_t* client,
                                          const char* name);

facebook::logdevice::Status ld_client_set_settings(logdevice_client_t* client,
                                                   const char* name,
                                                   const char* value);

HsInt ld_client_trim(logdevice_client_t* client, c_logid_t logid, c_lsn_t lsn,
                     HsStablePtr mvar, HsInt cap, c_error_code_t* st_out);

HsInt ld_client_find_time(logdevice_client_t* client, c_logid_t logid,
                          c_timestamp_t timestamp, HsInt accuracy,
                          HsStablePtr mvar, HsInt cap, c_error_code_t* st_out,
                          c_lsn_t* lsn_out);

// ----------------------------------------------------------------------------
// LogConfigType

facebook::logdevice::Status
ld_client_sync_logsconfig_version(logdevice_client_t* client, uint64_t version);

// LogDirectory
facebook::logdevice::Status
ld_client_make_directory_sync(logdevice_client_t* client, const char* path,
                              bool mk_intermediate_dirs, LogAttributes* attrs,
                              logdevice_logdirectory_t** logdir_ret);
facebook::logdevice::Status
ld_client_make_directory(logdevice_client_t* client, const char* path,
                         bool mk_intermediate_dirs, LogAttributes* attrs,
                         HsStablePtr mvar, HsInt cap,
                         make_directory_cb_data_t* data);

void free_logdevice_logdirectory(logdevice_logdirectory_t* dir);

facebook::logdevice::Status
ld_client_get_directory_sync(logdevice_client_t* client, const char* path,
                             logdevice_logdirectory_t** logdir_result);
facebook::logdevice::Status
ld_client_get_directory(logdevice_client_t* client, const char* path,
                        HsStablePtr mvar, HsInt cap,
                        facebook::logdevice::Status* st_out,
                        logdevice_logdirectory_t** logdir_result);

const LogAttributes* ld_logdirectory_get_attrs(logdevice_logdirectory_t* dir);

// LogGroup
facebook::logdevice::Status ld_client_make_loggroup_sync(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    logdevice_loggroup_t** loggroup_result);

facebook::logdevice::Status ld_client_make_loggroup(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    HsStablePtr mvar, HsInt cap, make_loggroup_cb_data_t* data);

void free_logdevice_loggroup(logdevice_loggroup_t* group);

facebook::logdevice::Status
ld_client_get_loggroup_sync(logdevice_client_t* client, const char* path,
                            logdevice_loggroup_t** loggroup_result);

void ld_client_get_loggroup(logdevice_client_t* client, const char* path,
                            HsStablePtr mvar, HsInt cap,
                            facebook::logdevice::Status* st_out,
                            logdevice_loggroup_t** loggroup_result);

facebook::logdevice::Status ld_client_rename(logdevice_client_t* client,
                                             const char* from_path,
                                             const char* to_path,
                                             HsStablePtr mvar, HsInt cap,
                                             logsconfig_status_cb_data_t* data);

facebook::logdevice::Status
ld_client_remove_loggroup(logdevice_client_t* client, const char* path,
                          HsStablePtr mvar, HsInt cap,
                          logsconfig_status_cb_data_t* data);

facebook::logdevice::Status
ld_client_set_log_group_range(logdevice_client_t* client, const char* path,
                              c_logid_t start, c_logid_t end, HsStablePtr mvar,
                              HsInt cap, logsconfig_status_cb_data_t* data);

facebook::logdevice::Status
ld_client_set_attributes(logdevice_client_t* client, const char* path,
                         LogAttributes* attrs, HsStablePtr mvar, HsInt cap,
                         logsconfig_status_cb_data_t* data);

//-----------------------------------------------------------------------------
// Log Head Attributes

void free_log_head_attributes(logdevice_log_head_attributes_t* p);
facebook::logdevice::Status
get_head_attributes(logdevice_client_t* client, c_logid_t logid,
                    HsStablePtr mvar, HsInt cap,
                    log_head_attributes_cb_data_t* data);
c_timestamp_t
get_trim_point_timestamp(logdevice_log_head_attributes_t* attribute);
lsn_t get_trim_point(logdevice_log_head_attributes_t* attribute);

// ----------------------------------------------------------------------------
// Appender

facebook::logdevice::Status
logdevice_append_async(logdevice_client_t* client, c_logid_t logid,
                       // payload
                       const char* payload, HsInt offset, HsInt length,
                       // cb
                       HsStablePtr mvar, HsInt cap,
                       logdevice_append_cb_data_t* cb_data);

facebook::logdevice::Status logdevice_append_with_attrs_async(
    logdevice_client_t* client, c_logid_t logid,
    // payload
    const char* payload, HsInt offset, HsInt length,
    // attr
    KeyType keytype, const char* keyval,
    // cb
    HsStablePtr mvar, HsInt cap, logdevice_append_cb_data_t* cb_data);

facebook::logdevice::Status
logdevice_append_sync(logdevice_client_t* client, c_logid_t logid,
                      // Payload
                      const char* payload, HsInt offset, HsInt length,
                      // Payload End
                      c_timestamp_t* ts, c_lsn_t* lsn_ret);

facebook::logdevice::Status
logdevice_append_with_attrs_sync(logdevice_client_t* client, c_logid_t logid,
                                 // Payload
                                 const char* payload, HsInt offset,
                                 HsInt length,
                                 // Payload End
                                 // optional_key
                                 KeyType keytype, const char* keyval,
                                 // OptionalKey End
                                 c_timestamp_t* ts, c_lsn_t* lsn_ret);

// ----------------------------------------------------------------------------
// Checkpoint

logdevice_checkpoint_store_t*
new_file_based_checkpoint_store(const char* root_path);

logdevice_checkpoint_store_t*
new_rsm_based_checkpoint_store(logdevice_client_t* client, c_logid_t log_id,
                               int64_t stop_timeout);
logdevice_checkpoint_store_t*
new_zookeeper_based_checkpoint_store(logdevice_client_t* client);

void free_checkpoint_store(logdevice_checkpoint_store_t* p);

void checkpoint_store_get_lsn(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              HsStablePtr mvar, HsInt cap,
                              facebook::logdevice::Status* st_out,
                              c_lsn_t* value_out);

facebook::logdevice::Status
checkpoint_store_get_lsn_sync(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              c_lsn_t* value_out);

void checkpoint_store_update_lsn(logdevice_checkpoint_store_t* store,
                                 const char* customer_id, c_logid_t logid,
                                 c_lsn_t lsn, HsStablePtr mvar, HsInt cap,
                                 facebook::logdevice::Status* st_out);

facebook::logdevice::Status
checkpoint_store_update_lsn_sync(logdevice_checkpoint_store_t* store,
                                 const char* customer_id, c_logid_t logid,
                                 c_lsn_t lsn);

facebook::logdevice::Status checkpoint_store_update_multi_lsn_sync(
    logdevice_checkpoint_store_t* store, const char* customer_id,
    c_logid_t* logids, c_lsn_t* lsns, size_t len);

// ----------------------------------------------------------------------------
// Reader

logdevice_reader_t* new_logdevice_reader(logdevice_client_t* client,
                                         size_t max_logs, ssize_t buffer_size);

void free_logdevice_reader(logdevice_reader_t* reader);

logdevice_sync_checkpointed_reader_t* new_sync_checkpointed_reader(
    const char* reader_name, logdevice_reader_t* reader,
    logdevice_checkpoint_store_t* store, uint32_t num_retries);

void free_sync_checkpointed_reader(logdevice_sync_checkpointed_reader_t* p);

facebook::logdevice::Status ld_reader_start_reading(logdevice_reader_t* reader,
                                                    c_logid_t logid,
                                                    c_lsn_t start,
                                                    c_lsn_t until);
facebook::logdevice::Status ld_checkpointed_reader_start_reading(
    logdevice_sync_checkpointed_reader_t* reader, c_logid_t logid,
    c_lsn_t start, c_lsn_t until);
facebook::logdevice::Status ld_checkpointed_reader_start_reading_from_ckp(
    logdevice_sync_checkpointed_reader_t* reader, c_logid_t logid,
    c_lsn_t until);

facebook::logdevice::Status ld_reader_stop_reading(logdevice_reader_t* reader,
                                                   c_logid_t logid);
facebook::logdevice::Status ld_checkpointed_reader_stop_reading(
    logdevice_sync_checkpointed_reader_t* reader, c_logid_t logid);

bool ld_reader_is_reading(logdevice_reader_t* reader, c_logid_t logid);
bool ld_checkpointed_reader_is_reading(
    logdevice_sync_checkpointed_reader_t* reader, c_logid_t logid);

bool ld_reader_is_reading_any(logdevice_reader_t* reader);
bool ld_checkpointed_reader_is_reading_any(
    logdevice_sync_checkpointed_reader_t* reader);

int ld_reader_set_timeout(logdevice_reader_t* reader, int32_t timeout);
int ld_checkpointed_reader_set_timeout(
    logdevice_sync_checkpointed_reader_t* reader, int32_t timeout);

facebook::logdevice::Status
logdevice_reader_read(logdevice_reader_t* reader, size_t maxlen,
                      logdevice_data_record_t* data_out,
                      logdevice_gap_record_t* gap_out, ssize_t* len_out);
facebook::logdevice::Status logdevice_checkpointed_reader_read(
    logdevice_sync_checkpointed_reader_t* reader, size_t maxlen,
    logdevice_data_record_t* data_out, logdevice_gap_record_t* gap_out,
    ssize_t* len_out);

void crb_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                           c_logid_t* logids, c_lsn_t* lsns, size_t len,
                           HsStablePtr mvar, HsInt cap,
                           facebook::logdevice::Status* st_out);

void crb_write_last_read_checkpoints(
    logdevice_sync_checkpointed_reader_t* reader, const c_logid_t* logids,
    size_t len, HsStablePtr mvar, HsInt cap,
    facebook::logdevice::Status* st_out);

facebook::logdevice::Status
sync_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                       c_logid_t* logids, c_lsn_t* lsns, size_t len);

facebook::logdevice::Status
sync_write_last_read_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                                 const c_logid_t* logids, size_t len);

// ----------------------------------------------------------------------------

#ifdef __cplusplus
} /* end extern "C" */
#endif
