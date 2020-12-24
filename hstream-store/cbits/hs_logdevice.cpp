#include "hs_logdevice.h"
#include "utils.h"

using facebook::logdevice::AppendAttributes;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::Reader;
using LogDirectory = facebook::logdevice::client::Directory;
using facebook::logdevice::ClientSettings;
using facebook::logdevice::client::LogAttributes;
using facebook::logdevice::client::LogGroup;

extern "C" {
// ----------------------------------------------------------------------------

struct logdevice_client_t {
  std::shared_ptr<Client> rep;
};
struct logdevice_reader_t {
  std::unique_ptr<Reader> rep;
};
struct logdevice_logdirectory_t {
  std::unique_ptr<LogDirectory> rep;
};
struct logdevice_loggroup_t {
  std::unique_ptr<LogGroup> rep;
};

void set_dbg_level_error(void) {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;
}

const char* show_error_name(facebook::logdevice::E err) {
  return facebook::logdevice::error_name(err);
}
const char* show_error_description(facebook::logdevice::E err) {
  return facebook::logdevice::error_description(err);
}

// TODO
// void init_logdevice(void) {
//  folly::SingletonVault::singleton()->registrationComplete();
//}

// ----------------------------------------------------------------------------

// new client
logdevice_client_t* new_logdevice_client(char* config_path) {
  std::shared_ptr<Client> client = ClientFactory().create(config_path);
  if (!client) {
    fprintf(stderr,
            "logdevice::ClientFactory().create() failed. Check the config "
            "path.\n");
    exit(1);
  }
  logdevice_client_t* result = new logdevice_client_t;
  result->rep = client;

  return result;
}
void free_logdevice_client(logdevice_client_t* client) { delete client; }

// new reader
logdevice_reader_t* new_logdevice_reader(logdevice_client_t* client,
                                         size_t max_logs, ssize_t buffer_size) {
  std::unique_ptr<Reader> reader;
  reader = client->rep->createReader(max_logs, buffer_size);
  logdevice_reader_t* result = new logdevice_reader_t;
  result->rep = std::move(reader);
  return result;
}
void free_logdevice_reader(logdevice_reader_t* reader) { delete reader; }

// ----------------------------------------------------------------------------
// Client

size_t ld_client_get_max_payload_size(logdevice_client_t* client) {
  return client->rep->getMaxPayloadSize();
}

facebook::logdevice::Status ld_client_set_settings(logdevice_client_t* client,
                                                   const char* name,
                                                   const char* value) {
  ClientSettings& settings = client->rep->settings();
  int ret = settings.set(name, value);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  else if (ret < 0) // On failure
    return facebook::logdevice::err;
  else {
    fprintf(stderr, "ld_client_set_settings :: Unexpected error happended!");
    exit(1);
  }
}

ClientSettings* create_default_client_settings() {
  return ClientSettings::create();
}

c_lsn_t ld_client_get_tail_lsn_sync(logdevice_client_t* client,
                                    uint64_t logid) {
  return client->rep->getTailLSNSync(facebook::logdevice::logid_t(logid));
}

// TODO
// logdevice_logdirectory_t*
// ld_client_make_directory_sync(logdevice_client_t* client, const char* path,
//                              bool mk_intermediate_dirs, char* failure_reason)
//                              {
//  std::unique_ptr<LogDirectory> directory;
//  std::string reason = failure_reason;
//  directory = client->rep->makeDirectorySync(path, mk_intermediate_dirs,
//                                             LogAttributes(), &reason);
//  logdevice_logdirectory_t* result = new logdevice_logdirectory_t;
//  result->rep = std::move(directory);
//  return result;
//}

// ----------------------------------------------------------------------------
// LogGroup

facebook::logdevice::Status ld_client_make_loggroup_sync(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    logdevice_loggroup_t** loggroup_result) {
  std::unique_ptr<LogGroup> loggroup = nullptr;
  auto start = facebook::logdevice::logid_t(start_logid);
  auto end = facebook::logdevice::logid_t(end_logid);
  std::string reason;

  loggroup = client->rep->makeLogGroupSync(
      path, std::make_pair(start, end), *attrs, mk_intermediate_dirs, &reason);
  if (loggroup) {
    logdevice_loggroup_t* result = new logdevice_loggroup_t;
    result->rep = std::move(loggroup);
    *loggroup_result = result;
    return facebook::logdevice::E::OK;
  }
  std::cerr << "-> ld_client_make_loggroup_sync error: " << reason << "\n";
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_get_loggroup_sync(logdevice_client_t* client, const char* path,
                            logdevice_loggroup_t** loggroup_result) {
  std::unique_ptr<LogGroup> loggroup = nullptr;
  std::string path_ = path;
  loggroup = client->rep->getLogGroupSync(path_);
  if (loggroup) {
    logdevice_loggroup_t* result = new logdevice_loggroup_t;
    result->rep = std::move(loggroup);
    *loggroup_result = result;
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_remove_loggroup_sync(logdevice_client_t* client, const char* path,
                               uint64_t* version) {
  std::string path_ = path;
  bool ret = client->rep->removeLogGroupSync(path_, version);
  if (ret)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

void ld_loggroup_get_range(logdevice_loggroup_t* group, c_logid_t* start,
                           c_logid_t* end) {
  const facebook::logdevice::logid_range_t& range = group->rep->range();
  *start = range.first.val();
  *end = range.second.val();
}

// NOTE: returned null-terminated string should be copied from ffi function.
const char* ld_loggroup_get_name(logdevice_loggroup_t* group) {
  return group->rep->name().c_str();
}

const char* ld_loggroup_get_fully_qualified_name(logdevice_loggroup_t* group) {
  return group->rep->getFullyQualifiedName().c_str();
}

uint64_t ld_loggroup_get_version(logdevice_loggroup_t* group) {
  return group->rep->version();
}

void* free_lodevice_loggroup(logdevice_loggroup_t* group) { delete group; }

// LogGroup END
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// Writer

c_lsn_t logdevice_append_sync(logdevice_client_t* client, c_logid_t logid,
                              const char* payload, HsInt offset, HsInt length,
                              int64_t* ts) {
  c_lsn_t result;
  std::string user_payload(payload + offset, length);
  if (ts) {
    std::chrono::milliseconds timestamp;
    result =
        client->rep->appendSync(facebook::logdevice::logid_t(logid),
                                user_payload, AppendAttributes(), &timestamp);
    *ts = timestamp.count();
  } else {
    result = client->rep->appendSync(facebook::logdevice::logid_t(logid),
                                     user_payload, AppendAttributes(), nullptr);
  }
  return result;
}

// ----------------------------------------------------------------------------
// Reader

int logdevice_reader_start_reading(logdevice_reader_t* reader, c_logid_t logid,
                                   c_lsn_t start, c_lsn_t until) {
  return reader->rep->startReading(facebook::logdevice::logid_t(logid), start,
                                   until);
}
bool logdevice_reader_is_reading(logdevice_reader_t* reader, c_logid_t logid) {
  return reader->rep->isReading(facebook::logdevice::logid_t(logid));
}
bool logdevice_reader_is_reading_any(logdevice_reader_t* reader) {
  return reader->rep->isReadingAny();
}
int logdevice_reader_read_sync(logdevice_reader_t* reader, size_t maxlen,
                               logdevice_data_record_t* data_out,
                               ssize_t* len_out) {
  std::vector<std::unique_ptr<facebook::logdevice::DataRecord>> data;
  facebook::logdevice::GapRecord gap;

  ssize_t nread = reader->rep->read(maxlen, &data, &gap);
  *len_out = nread;
  // Copy data record
  if (nread >= 0) {
    int i = 0;
    for (auto& record_ptr : data) {
      const facebook::logdevice::Payload& payload = record_ptr->payload;
      const facebook::logdevice::DataRecordAttributes& attrs =
          record_ptr->attrs;
      facebook::logdevice::logid_t& logid = record_ptr->logid;
      data_out[i].logid = logid.val_;
      data_out[i].lsn = attrs.lsn;
      data_out[i].payload = copyString(payload.toString());
      data_out[i].payload_len = payload.size();
      i += 1;
    }
  }
  // A gap in the numbering sequence.  Warn about data loss but ignore
  // other types of gaps.
  else {
    if (gap.type == facebook::logdevice::GapType::DATALOSS) {
      fprintf(stderr, "warning: DATALOSS gaps for LSN range [%ld, %ld]\n",
              gap.lo, gap.hi);
      return 1;
    }
  }
  return 0;
}

// ----------------------------------------------------------------------------
} // end extern "C"
