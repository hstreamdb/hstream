#include "hs_logdevice.h"
#include "utils.h"

extern "C" {
// ----------------------------------------------------------------------------

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
facebook::logdevice::Status
new_logdevice_client(char* config_path, logdevice_client_t** client_ret) {
  std::shared_ptr<Client> client = ClientFactory().create(config_path);
  if (client) {
    logdevice_client_t* result = new logdevice_client_t;
    result->rep = client;
    *client_ret = result;
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
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

const std::string* ld_client_get_settings(logdevice_client_t* client,
                                          const char* name) {
  auto value = new std::string;
  ClientSettings& settings = client->rep->settings();
  folly::Optional<std::string> maybe_value = settings.get(name);
  *value = maybe_value.value_or(nullptr);
  return value;
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
  std::vector<std::unique_ptr<DataRecord>> data;
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
