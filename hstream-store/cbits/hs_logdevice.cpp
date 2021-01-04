#include "hs_logdevice.h"

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
} // end extern "C"
