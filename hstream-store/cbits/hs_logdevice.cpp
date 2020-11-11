#include <cstdlib>
#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/Err.h>
#include <logdevice/include/Reader.h>
#include <logdevice/include/Record.h>
#include <logdevice/include/RecordOffset.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>

#include "hs_logdevice.h"

using facebook::logdevice::AppendAttributes;
using facebook::logdevice::Client;
using facebook::logdevice::ClientFactory;
using facebook::logdevice::logid_t;
using facebook::logdevice::lsn_t;
using facebook::logdevice::Reader;

extern "C" {

struct logdevice_client_t {
  std::shared_ptr<Client> rep;
};
struct logdevice_reader_t {
  std::unique_ptr<Reader> rep;
};

// ----------------------------------------------------------------------------

void set_dbg_level_error(void) {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;
}
void init_logdevice(void) {
  folly::SingletonVault::singleton()->registrationComplete();
}

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
                                         size_t max_logs,
                                         ssize_t *buffer_size) {
  std::unique_ptr<Reader> reader;
  if (buffer_size)
    reader = client->rep->createReader(max_logs, *buffer_size);
  else
    reader = client->rep->createReader(max_logs);
  logdevice_reader_t *result = new logdevice_reader_t;
  result->rep = std::move(reader);
  return result;
}
void free_logdevice_reader(logdevice_reader_t *reader) { delete reader; }

// ----------------------------------------------------------------------------
// Writer

lsn_t append_sync(logdevice_client_t *client, uint64_t logid,
                  const char *payload, int64_t *ts) {
  lsn_t result;
  if (ts) {
    std::chrono::milliseconds timestamp;
    result = client->rep->appendSync(logid_t(logid), payload,
                                     AppendAttributes(), &timestamp);
    *ts = timestamp.count();
  } else {
    result = client->rep->appendSync(logid_t(logid), payload,
                                     AppendAttributes(), nullptr);
  }
  return result;
}

// ----------------------------------------------------------------------------
// Reader
// TODO

// ----------------------------------------------------------------------------
} // end extern "C"
