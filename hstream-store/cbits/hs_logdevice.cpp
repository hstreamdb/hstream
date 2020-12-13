#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/Err.h>
#include <logdevice/include/LogsConfigTypes.h>
#include <logdevice/include/Reader.h>
#include <logdevice/include/Record.h>
#include <logdevice/include/RecordOffset.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>

#include "hs_logdevice.h"

using facebook::logdevice::AppendAttributes;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::Reader;

extern "C" {
// ----------------------------------------------------------------------------
// Helpers
static char *copyString(const std::string &str) {
  char *result = reinterpret_cast<char *>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

// ----------------------------------------------------------------------------

struct logdevice_client_t {
  std::shared_ptr<Client> rep;
};
struct logdevice_reader_t {
  std::unique_ptr<Reader> rep;
};

const c_logid_t C_LOGID_INVALID =
    facebook::logdevice::LOGID_INVALID.val();
const c_logid_t C_LOGID_INVALID2 =
    facebook::logdevice::LOGID_INVALID2.val();
// max valid data logid value. This accounts for internal logs.
// Not to be confused with numeric_limits<>::max().
const c_logid_t C_LOGID_MAX = facebook::logdevice::LOGID_MAX.val();
// max valid user data logid value.
const c_logid_t C_USER_LOGID_MAX =
    facebook::logdevice::USER_LOGID_MAX.val();
// maximum number of bits in a log id
const size_t C_LOGID_MAX_BITS = facebook::logdevice::LOGID_BITS;

void set_dbg_level_error(void) {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;
}
// TODO
// void init_logdevice(void) {
//  folly::SingletonVault::singleton()->registrationComplete();
//}

// ----------------------------------------------------------------------------
// Init & Destroy

logdevice_client_t *new_logdevice_client(char *config_path) {
  std::shared_ptr<Client> client = ClientFactory().create(config_path);
  if (!client) {
    fprintf(stderr,
            "logdevice::ClientFactory().create() failed. Check the config "
            "path.\n");
    exit(1);
  }
  logdevice_client_t *result = new logdevice_client_t;
  result->rep = client;

  return result;
}
void free_logdevice_client(logdevice_client_t *client) { delete client; }

logdevice_reader_t *new_logdevice_reader(logdevice_client_t *client,
                                         size_t max_logs, ssize_t buffer_size) {
  std::unique_ptr<Reader> reader;
  reader = client->rep->createReader(max_logs, buffer_size);
  logdevice_reader_t *result = new logdevice_reader_t;
  result->rep = std::move(reader);
  return result;
}
void free_logdevice_reader(logdevice_reader_t *reader) { delete reader; }

// ----------------------------------------------------------------------------

c_lsn_t logdevice_get_tail_lsn_sync(logdevice_client_t *client,
                                    uint64_t logid) {
  return client->rep->getTailLSNSync(facebook::logdevice::logid_t(logid));
}

// ----------------------------------------------------------------------------
// Writer

c_lsn_t logdevice_append_sync(logdevice_client_t *client, uint64_t logid,
                              const char *payload, HsInt offset, HsInt length,
                              int64_t *ts) {
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

int logdevice_reader_start_reading(logdevice_reader_t *reader, c_logid_t logid,
                                   c_lsn_t start, c_lsn_t until) {
  return reader->rep->startReading(facebook::logdevice::logid_t(logid), start,
                                   until);
}
bool logdevice_reader_is_reading(logdevice_reader_t *reader, c_logid_t logid) {
  return reader->rep->isReading(facebook::logdevice::logid_t(logid));
}
bool logdevice_reader_is_reading_any(logdevice_reader_t *reader) {
  return reader->rep->isReadingAny();
}
int logdevice_reader_read_sync(logdevice_reader_t *reader, size_t maxlen,
                               logdevice_data_record_t *data_out,
                               ssize_t *len_out) {
  std::vector<std::unique_ptr<facebook::logdevice::DataRecord>> data;
  facebook::logdevice::GapRecord gap;

  ssize_t nread = reader->rep->read(maxlen, &data, &gap);
  *len_out = nread;
  // Copy data record
  if (nread >= 0) {
    int i = 0;
    for (auto &record_ptr : data) {
      const facebook::logdevice::Payload &payload = record_ptr->payload;
      const facebook::logdevice::DataRecordAttributes &attrs =
          record_ptr->attrs;
      facebook::logdevice::logid_t &logid = record_ptr->logid;
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
