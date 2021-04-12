#include "hs_logdevice.h"

extern "C" {
// ----------------------------------------------------------------------------

int new_logdevice_admin_async_client(
    const char* host, uint16_t port, bool allow_name_lookup,
    uint32_t channel_timeout, logdevice_admin_async_client_t** client_ret) {
  folly::SocketAddress address(host, port, allow_name_lookup);
  auto transport = folly::AsyncSocket::newSocket(
      folly::EventBaseManager::get()->getEventBase(), address);
  auto channel =
      apache::thrift::HeaderClientChannel::newChannel(std::move(transport));
  channel->setTimeout(channel_timeout);
  if (!channel->good()) {
    return -1;
  }
  auto client = std::make_unique<AdminAPIAsyncClient>(std::move(channel));
  logdevice_admin_async_client_t* result = new logdevice_admin_async_client_t;
  result->rep = std::move(client);
  *client_ret = result;
  return 0;
}

void free_logdevice_admin_async_client(logdevice_admin_async_client_t* p) {
  delete p;
}

thrift_rpc_options_t* new_thrift_rpc_options(int64_t timeout) {
  auto options = std::make_unique<apache::thrift::RpcOptions>();
  options->setTimeout(std::chrono::milliseconds(timeout));
  thrift_rpc_options_t* result = new thrift_rpc_options_t;
  result->rep = std::move(options);
  return result;
}

void free_thrift_rpc_options(thrift_rpc_options_t* p) { delete p; }

std::string* ld_admin_sync_getVersion(logdevice_admin_async_client_t* client,
                                      thrift_rpc_options_t* rpc_options) {
  auto version = new std::string;
  client->rep->sync_getVersion(*(rpc_options->rep), *version);
  return version;
}

fb_status ld_admin_sync_getStatus(logdevice_admin_async_client_t* client,
                                  thrift_rpc_options_t* rpc_options) {
  return client->rep->sync_getStatus(*(rpc_options->rep));
}

int64_t ld_admin_sync_aliveSince(logdevice_admin_async_client_t* client,
                                 thrift_rpc_options_t* rpc_options) {
  return client->rep->sync_aliveSince(*(rpc_options->rep));
}

int64_t ld_admin_sync_getPid(logdevice_admin_async_client_t* client,
                             thrift_rpc_options_t* rpc_options) {
  return client->rep->sync_getPid(*(rpc_options->rep));
}

// ----------------------------------------------------------------------------
} // end extern "C"
