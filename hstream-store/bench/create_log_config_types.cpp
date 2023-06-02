// g++ -std=c++17 hstream-store/bench/create_log_config_types.cpp \
//   -lfolly -llogdevice -o /tmp/create_log_config_types

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include <folly/Singleton.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"

using facebook::logdevice::logid_range_t;
using facebook::logdevice::logid_t;
using facebook::logdevice::client::LogAttributes;
using namespace std::chrono;

int main(int argc, const char* argv[]) {
  folly::SingletonVault::singleton()->registrationComplete();

  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::ERROR;

  std::string config_path = "local-data/logdevice/logdevice.conf";

  std::shared_ptr<facebook::logdevice::Client> client =
      facebook::logdevice::ClientFactory().create(config_path);
  if (!client) {
    fprintf(stderr,
            "logdevice::ClientFactory::create() failed.  Is the config path "
            "correct?\n");
    exit(1);
  }

  auto root = "/tmp/cpp/";
  for (int i = 1; i <= 1000; ++i) {
    auto start = high_resolution_clock::now();
    for (int j = 1; j <= 1000; ++j) {
      auto path = root + std::to_string(i) + "_" + std::to_string(j);
      // std::string failure_reason = "";
      // auto dir = client->makeDirectorySync(path, true, LogAttributes(),
      // &failure_reason); if (!dir) {
      //   std::cout << failure_reason << std::endl;
      //   exit(1);
      // }
      auto range = std::make_pair(logid_t(i * 1000 + j), logid_t(i * 1000 + j));
      std::string failure_reason = "";
      auto loggroup = client->makeLogGroupSync(
          path, range, LogAttributes().with_replicationFactor(1), true,
          &failure_reason);
      if (!loggroup) {
        std::cout << failure_reason << std::endl;
        exit(1);
      }
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    std::cout << "=> " << duration.count() << std::endl;
  }
}
