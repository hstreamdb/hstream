#include "hs_logdevice.h"

extern "C" {
// ----------------------------------------------------------------------------

void set_dbg_level(c_logdevice_dbg_level level) {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level(level);
}

int dbg_use_fd(int fd) { return facebook::logdevice::dbg::useFD(fd); }

const char* show_error_name(facebook::logdevice::E err) {
  return facebook::logdevice::error_name(err);
}
const char* show_error_description(facebook::logdevice::E err) {
  return facebook::logdevice::error_description(err);
}

void init_logdevice(void) {
  folly::SingletonVault::singleton()->registrationComplete();
}

// ----------------------------------------------------------------------------
// Client

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

HsInt ld_client_trim(logdevice_client_t* client, c_logid_t logid, c_lsn_t lsn,
                     HsStablePtr mvar, HsInt cap, c_error_code_t* st_out) {
  auto cb = [st_out, cap, mvar](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = static_cast<c_error_code_t>(st);
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  return client->rep->trim(logid_t(logid), lsn, cb);
}

HsInt ld_client_find_time(logdevice_client_t* client, c_logid_t logid,
                          c_timestamp_t timestamp, c_accuracy_t accuracy,
                          HsStablePtr mvar, HsInt cap, c_error_code_t* st_out,
                          c_lsn_t* lsn_out) {
  auto cb = [st_out, lsn_out, cap, mvar](facebook::logdevice::Status st,
                                         c_lsn_t lsn) {
    if (st_out && lsn_out) {
      *st_out = static_cast<c_error_code_t>(st);
      *lsn_out = lsn;
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  return client->rep->findTime(logid_t(logid),
      std::chrono::milliseconds(timestamp), cb,
      static_cast<facebook::logdevice::FindKeyAccuracy>(accuracy));
}

// ----------------------------------------------------------------------------
} // end extern "C"
