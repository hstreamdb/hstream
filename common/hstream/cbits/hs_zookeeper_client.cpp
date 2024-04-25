#include <logdevice/common/ZookeeperClient.h>
#include <zookeeper/zookeeper.h>

// We directly use the ZookeeperClient class from LogDevice, it handles
// SessionExpire that we need to handle.
//
// TODO:
//
// 1. Currently logging messages are printed by ld_info, which is controlled by
// `--store-log-level`. We need to find a way to use the hstream logger.

extern "C" {

typedef struct zookeeper_client_t {
  std::unique_ptr<facebook::logdevice::ZookeeperClient> rep;
} zookeeper_client_t;

zookeeper_client_t* new_zookeeper_client(const char* quorum,
                                         int session_timeout) {
  zookeeper_client_t* client = new zookeeper_client_t();
  client->rep = std::make_unique<facebook::logdevice::ZookeeperClient>(
      std::string(quorum), std::chrono::milliseconds(session_timeout));
  return client;
}

zhandle_t* get_underlying_handle(zookeeper_client_t* client) {
  return client->rep->getHandle().get();
}

void delete_zookeeper_client(zookeeper_client_t* client) { delete client; }

// end extern "C"
}
