// g++ -std=c++17 hstream-store/bench/checkpoint_store.cpp \
//    -lfolly -llogdevice -o /tmp/checkpoint_store

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include <folly/Singleton.h>

#include <logdevice/include/CheckpointStore.h>
#include <logdevice/include/CheckpointStoreFactory.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/debug.h>
#include <logdevice/include/types.h>

using namespace facebook::logdevice;
using namespace std::chrono;
using namespace std::chrono_literals;

void bench_checkpoint_store(std::unique_ptr<CheckpointStore>&& checkpoint_store,
                            int x, int y) {
  Status status;
  for (int i = 1; i <= x; i++) {
    auto start = high_resolution_clock::now();
    for (int j = 1; j <= y; ++j) {
      int retry = 1;
      while (retry) {
        // Status status = checkpoint_store->updateLSNSync("x", logid_t(1), 1);
        // Status status = checkpoint_store->updateLSNSync("x", logid_t(j), 1);
        status = checkpoint_store->updateLSNSync("x", logid_t(i * 1000 + j), 1);
        // status = checkpoint_store->updateLSNSync(std::to_string(i * 1000 +
        // j),
        //                                          logid_t(1), 1);
        // status = checkpoint_store->updateLSNSync(
        //     std::string(logid_t(i * 1000 + j)), logid_t(i * 1000 + j), 1);
        if (status == Status::OK) {
          break;
        } else if (status == Status::AGAIN) {
          retry++;
          printf("retry %d\n", retry);
          std::this_thread::sleep_for(500ms);
        } else {
          fprintf(stderr, "Update failed\n");
          std::cout << status << std::endl;
          exit(1);
        }
      }
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    std::cout << "=> " << i << " " << duration.count() / 1000000 << "s"
              << std::endl;
  }
}

void rsm_based(std::shared_ptr<facebook::logdevice::Client> client,
               logid_t logid) {
  auto stop_timeout = std::chrono::milliseconds(5000);
  client->trimSync(logid, LSN_OLDEST); // Reset

  std::unique_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createRSMBasedCheckpointStore(client, logid,
                                                             stop_timeout);
  bench_checkpoint_store(std::move(checkpoint_store), 1000, 1000);

  /*
  auto until_lsn = client->getTailLSNSync(logid);
  client->trimSync(logid, until_lsn - 1);

  for (int i = 0; i < 20; i++) {
    int retry = 1;
    auto start = high_resolution_clock::now();
    while (retry) {
      status = checkpoint_store->updateLSNSync("x", logid_t(10000), 1);
      if (status == Status::OK) {
        break;
      } else if (status == Status::AGAIN) {
        retry++;
        printf("retry %d\n", retry);
        std::this_thread::sleep_for(500ms);
      } else {
        fprintf(stderr, "Update failed\n");
        std::cout << status << std::endl;
        exit(1);
      }
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    std::cout << "=> " << duration.count() << std::endl;
  }
  */
}

void zk_based(std::shared_ptr<facebook::logdevice::Client> client) {
  auto checkpoint_store =
      CheckpointStoreFactory().createZookeeperBasedCheckpointStore(client);
  bench_checkpoint_store(std::move(checkpoint_store), 1000, 1000);
}

void usage() {
  printf("bench_checkpoint_store <ld_config_path> [rsm|zk] ...\n");
  printf("For rsm based:\n");
  printf("  The last arg is logid\n");
  printf("\n");
  // printf("For zk based:\n");
  printf("\n");
}

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::CRITICAL;

  if (argc <= 2) {
    usage();
    exit(1);
  }
  int arg_idx = 1;
  // e.g. "local-data/logdevice/logdevice.conf"
  std::string config_path = argv[arg_idx++];
  // e.g. rsm or zk
  std::string ckp_type = argv[arg_idx++];
  // TODO e.g. 1
  // std::string bench_mode = argv[arg_idx++];

  std::shared_ptr<facebook::logdevice::Client> client =
      facebook::logdevice::ClientFactory().create(config_path);
  if (!client) {
    fprintf(stderr,
            "logdevice::ClientFactory::create() failed. Is the config path "
            "correct?\n");
    exit(1);
  }

  if (ckp_type == "rsm") {
    logid_t logid = logid_t(std::stoi(argv[arg_idx++]));
    rsm_based(client, logid);
  } else if (ckp_type == "zk") {
    zk_based(client);
  } else {
    usage();
    exit(1);
  }

  return 0;
}
