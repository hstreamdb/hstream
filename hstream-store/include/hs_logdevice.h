#include <HsFFI.h>
#include <iostream>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <folly/String.h>
#include <folly/io/async/AsyncSocket.h>
#include <logdevice/admin/if/gen-cpp2/AdminAPI.h>
#include <logdevice/include/CheckpointStore.h>
#include <logdevice/include/CheckpointStoreFactory.h>
#include <logdevice/include/CheckpointedReaderBase.h>
#include <logdevice/include/CheckpointedReaderFactory.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/ClientSettings.h>
#include <logdevice/include/Err.h>
#include <logdevice/include/LogAttributes.h>
#include <logdevice/include/LogsConfigTypes.h>
#include <logdevice/include/Reader.h>
#include <logdevice/include/Record.h>
#include <logdevice/include/RecordOffset.h>
#include <logdevice/include/SyncCheckpointedReader.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>
#include <thrift/lib/cpp/async/TAsyncSSLSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#ifndef HS_LOGDEVICE
#define HS_LOGDEVICE

using facebook::logdevice::AppendAttributes;
using facebook::logdevice::CheckpointedReaderBase;
using facebook::logdevice::CheckpointedReaderFactory;
using facebook::logdevice::CheckpointStore;
using facebook::logdevice::CheckpointStoreFactory;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::ClientSettings;
using facebook::logdevice::DataRecord;
using facebook::logdevice::KeyType;
using facebook::logdevice::logid_t;
using facebook::logdevice::lsn_t;
using facebook::logdevice::Payload;
using facebook::logdevice::Reader;
using facebook::logdevice::SyncCheckpointedReader;
using facebook::logdevice::client::LogAttributes;
using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
using facebook::logdevice::client::LogGroup;
using LogDirectory = facebook::logdevice::client::Directory;
using facebook::fb303::cpp2::fb_status;
using facebook::logdevice::thrift::AdminAPIAsyncClient;

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t c_timestamp_t;
typedef uint16_t c_error_code_t;
typedef uint8_t c_keytype_t;

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
struct logdevice_sync_checkpointed_reader_t {
  std::unique_ptr<SyncCheckpointedReader> rep;
};
struct logdevice_checkpoint_store_t {
  std::unique_ptr<CheckpointStore> rep;
};
typedef struct logdevice_admin_async_client_t {
  std::unique_ptr<AdminAPIAsyncClient> rep;
} logdevice_admin_async_client_t;
typedef struct thrift_rpc_options_t {
  std::unique_ptr<apache::thrift::RpcOptions> rep;
} thrift_rpc_options_t;

typedef struct logdevice_loggroup_t logdevice_loggroup_t;
typedef struct logdevice_logdirectory_t logdevice_logdirectory_t;
typedef struct logdevice_client_t logdevice_client_t;
typedef struct logdevice_checkpoint_store_t logdevice_checkpoint_store_t;
typedef struct logdevice_reader_t logdevice_reader_t;
typedef struct logdevice_sync_checkpointed_reader_t
    logdevice_sync_checkpointed_reader_t;

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
  c_logid_t logid;
  c_lsn_t lsn;
  char* payload;
  size_t payload_len;
} logdevice_data_record_t;

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

// AppendCallbackData
typedef struct logdevice_append_cb_data_t {
  c_error_code_t st;
  c_logid_t logid;
  c_lsn_t lsn;
  c_timestamp_t timestamp;
} logdevice_append_cb_data_t;

// ----------------------------------------------------------------------------
// Client

facebook::logdevice::Status
new_logdevice_client(char* config_path, logdevice_client_t** client_ret);

void free_logdevice_client(logdevice_client_t* client);

size_t ld_client_get_max_payload_size(logdevice_client_t* client);

c_lsn_t ld_client_get_tail_lsn_sync(logdevice_client_t* client, uint64_t logid);

const std::string* ld_client_get_settings(logdevice_client_t* client,
                                          const char* name);

facebook::logdevice::Status ld_client_set_settings(logdevice_client_t* client,
                                                   const char* name,
                                                   const char* value);

// ----------------------------------------------------------------------------
// LogConfigType

facebook::logdevice::Status
ld_client_sync_logsconfig_version(logdevice_client_t* client, uint64_t version);

// LogDirectory
facebook::logdevice::Status
ld_client_make_directory_sync(logdevice_client_t* client, const char* path,
                              bool mk_intermediate_dirs, LogAttributes* attrs,
                              logdevice_logdirectory_t** logdir_ret);

void free_logdevice_logdirectory(logdevice_logdirectory_t* dir);

const char* ld_logdirectory_get_name(logdevice_logdirectory_t* dir);
uint64_t ld_logdirectory_get_version(logdevice_logdirectory_t* dir);

// LogGroup
facebook::logdevice::Status ld_client_make_loggroup_sync(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    logdevice_loggroup_t** loggroup_result);

void free_logdevice_loggroup(logdevice_loggroup_t* group);

facebook::logdevice::Status
ld_client_get_loggroup_sync(logdevice_client_t* client, const char* path,
                            logdevice_loggroup_t** loggroup_result);

void ld_loggroup_get_range(logdevice_loggroup_t* group, c_logid_t* start,
                           c_logid_t* end);
const char* ld_loggroup_get_name(logdevice_loggroup_t* group);
const char* ld_loggroup_get_fully_qualified_name(logdevice_loggroup_t* group);
uint64_t ld_loggroup_get_version(logdevice_loggroup_t* group);

// ----------------------------------------------------------------------------
// Appender

facebook::logdevice::Status
logdevice_append_async(HsStablePtr mvar, HsInt cap,
                       logdevice_append_cb_data_t* cb_data,
                       logdevice_client_t* client, c_logid_t logid,
                       const char* payload, HsInt offset, HsInt length);

facebook::logdevice::Status logdevice_append_with_attrs_async(
    HsStablePtr mvar, HsInt cap, logdevice_append_cb_data_t* cb_data,
    logdevice_client_t* client, c_logid_t logid, const char* payload,
    HsInt offset, HsInt length, KeyType keytype, const char* keyval);

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

void free_checkpoint_store(logdevice_checkpoint_store_t* p);

facebook::logdevice::Status
checkpoint_store_get_lsn_sync(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              c_lsn_t* value_out);

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
                      logdevice_data_record_t* data_out, ssize_t* len_out);
facebook::logdevice::Status logdevice_checkpointed_reader_read(
    logdevice_sync_checkpointed_reader_t* reader, size_t maxlen,
    logdevice_data_record_t* data_out, ssize_t* len_out);

facebook::logdevice::Status
sync_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                       c_logid_t* logids, c_lsn_t* lsns, size_t len);

facebook::logdevice::Status
sync_write_last_read_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                                 const c_logid_t* logids, size_t len);

// ----------------------------------------------------------------------------
// Admin Client

int new_logdevice_admin_async_client(
    const char* host, uint16_t port, bool allow_name_lookup,
    uint32_t channel_timeout, logdevice_admin_async_client_t** client_ret);

void free_logdevice_admin_async_client(logdevice_admin_async_client_t* p);

thrift_rpc_options_t* new_thrift_rpc_options(int64_t timeout);

void free_thrift_rpc_options(thrift_rpc_options_t* p);

std::string* ld_admin_sync_getVersion(logdevice_admin_async_client_t* client,
                                      thrift_rpc_options_t* rpc_options);

fb_status ld_admin_sync_getStatus(logdevice_admin_async_client_t* client,
                                  thrift_rpc_options_t* rpc_options);

int64_t ld_admin_sync_aliveSince(logdevice_admin_async_client_t* client,
                                 thrift_rpc_options_t* rpc_options);

int64_t ld_admin_sync_getPid(logdevice_admin_async_client_t* client,
                             thrift_rpc_options_t* rpc_options);
// ---------------------------------------------------------------------- ------

#ifdef __cplusplus
} /* end extern "C" */
#endif

// End define HS_LOGDEVICE
#endif
