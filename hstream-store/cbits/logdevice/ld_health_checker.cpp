#include <chrono>
#include <cstdlib>
#include <string>
#include <unordered_set>

#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/nodes/NodeAttributesConfig.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/debug.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientImpl.h"

#include "hs_logdevice.h"

using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice;

class LdChecker {
public:
  explicit LdChecker(std::shared_ptr<Client> client)
      : client_(std::move(client)) {}

  LdChecker(const LdChecker&) = delete;
  LdChecker& operator=(const LdChecker&) = delete;

  LdChecker(LdChecker&&) = delete;
  LdChecker& operator=(LdChecker&&) = delete;

  ~LdChecker() = default;

  auto check(int unhealthy_node_limit) -> bool {
    auto* client_impl = dynamic_cast<ClientImpl*>(client_.get());
    auto config = client_impl->getConfig();
    const auto nodes_configuration = config->getNodesConfiguration();
    auto sdc = nodes_configuration->getServiceDiscovery();

    auto start = std::chrono::steady_clock::now();
    getClusterState(*client_impl, *nodes_configuration);
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    ld_warning("GetClusterState took %ld ms", duration);

    // getClusterState(*client_impl, *nodes_configuration);

    auto total_nodes = sdc->numNodes();
    auto unhealthy_nodes_set = std::unordered_set<node_index_t>(total_nodes);

    std::lock_guard<std::mutex> guard(mutex_);
    for (auto& res : stateResults_) {
      if (res.status == E::OK) {
        for (auto& [node_idx, state] : res.nodes_state) {
          if (sdc->hasNode(node_idx) && state != 0) {
            unhealthy_nodes_set.insert(node_idx);
          }
          if (sdc->hasNode(node_idx) && state == 3) {
            unhealthy_nodes_set.insert(node_idx);
          }
        }
      }
    }

    if (!unhealthy_nodes_set.empty()) {
      ld_warning("Cluster has %lu unhealthy nodes:",
                 unhealthy_nodes_set.size());
      // printUnhealthyNodes(*nodes_configuration, unhealthy_nodes_set);
    }

    return unhealthy_nodes_set.size() <= unhealthy_node_limit;
  }

private:
  struct ClusterStateResult {
    node_index_t node_id;
    std::string addr;
    Status status;
    std::vector<std::pair<node_index_t, uint16_t>> nodes_state;
    std::vector<std::pair<node_index_t, uint16_t>> nodes_status;
  };

  // need acquire lock before print
  auto printUnhealthyNodes(const NodesConfiguration& nodes_configuration,
                           const std::unordered_set<node_index_t>& sets)
      -> void {
    for (auto& res : stateResults_) {
      auto st = "ALIVE";
      if (res.status == E::TIMEDOUT) {
        st = "SUSPECT";
      } else if (res.status != E::OK) {
        st = "DEAD";
      }
      auto deadNodes = nodesStateToString(nodes_configuration, res.nodes_state);
      auto unhealthyNodes =
          nodesStatusToUnhealthy(nodes_configuration, res.nodes_status);
      ld_warning("[Node %d(%s)]: status: %s, dead nodes index: [%s], "
                 "unhealthy nodes index: [%s]",
                 res.node_id, res.addr.c_str(), st, deadNodes.c_str(),
                 unhealthyNodes.c_str());
    }
    ld_warning("Check return unhealthy nodes: [%s]",
               folly::join(',', sets).c_str());
  }

  auto addResult(node_index_t node_id, std::string address, Status status,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_state,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_status)
      -> void {
    ClusterStateResult res;
    res.node_id = node_id;
    res.addr = address;
    res.status = status;
    res.nodes_state = std::move(nodes_state);
    res.nodes_status = std::move(nodes_status);

    {
      std::lock_guard<std::mutex> guard(mutex_);
      stateResults_.push_back(std::move(res));
    }
  }

  auto clearResults() -> void {
    std::lock_guard<std::mutex> guard(mutex_);
    stateResults_.clear();
  }

  auto getClusterState(const ClientImpl& client_impl,
                       const NodesConfiguration& nodes_configuration) -> void {
    int posted_requests = 0;
    auto sdc = nodes_configuration.getServiceDiscovery();
    clearResults();

    facebook::logdevice::Semaphore sem;

    for (auto it = sdc->begin(); it != sdc->end(); ++it) {
      auto node_id = it->first;
      auto attr = it->second;
      auto addr = attr.default_client_data_address.toString();

      auto cb =
          [&sem, node_id, addr,
           this](Status status,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_state,
                 std::vector<node_index_t> boycotted_nodes,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_status) {
            addResult(node_id, addr, status, nodes_state, nodes_status);
            sem.post();
          };

      std::unique_ptr<Request> req = std::make_unique<GetClusterStateRequest>(
          std::chrono::milliseconds(2000), // 2s timeout
          std::chrono::seconds(60),        // wave timeout is useless here
          std::move(cb), nodes_configuration.getNodeID(node_id));

      auto rv = client_impl.getProcessor().postImportant(req);
      ld_check(rv == 0);
      ++posted_requests;
    }

    while (posted_requests > 0) {
      sem.wait();
      --posted_requests;
    }
  }

  static auto nodesStateToString(
      const NodesConfiguration& nodes_configuration,
      const std::vector<std::pair<node_index_t, uint16_t>>& nodes_state)
      -> std::string {
    auto sdc = *nodes_configuration.getServiceDiscovery();
    std::vector<facebook::logdevice::node_index_t> dead_nodes;
    if (!nodes_state.empty()) {
      for (auto& [node_idx, state] : nodes_state) {
        if (sdc.hasNode(node_idx) && state != 0) {
          dead_nodes.push_back(node_idx);
        }
      }
    }

    return folly::join(',', dead_nodes);
  }

  static auto nodesStatusToUnhealthy(
      const NodesConfiguration& nodes_configuration,
      const std::vector<std::pair<node_index_t, uint16_t>>& nodes_status)
      -> std::string {
    auto sdc = *nodes_configuration.getServiceDiscovery();
    std::vector<node_index_t> unhealthy_nodes;
    if (!nodes_status.empty()) {
      for (auto& [node_idx, status] : nodes_status) {
        if (sdc.hasNode(node_idx) && status == 3) {
          unhealthy_nodes.push_back(node_idx);
        }
      }
    }
    return folly::join(',', unhealthy_nodes);
  }

  std::shared_ptr<Client> client_;
  std::vector<ClusterStateResult> stateResults_;
  std::mutex mutex_;
};

extern "C" {

struct ld_checker {
  std::unique_ptr<LdChecker> rep;
};

ld_checker* new_ld_checker(logdevice_client_t* client) {
  auto checker = std::make_unique<LdChecker>(client->rep);
  ld_checker* res = new ld_checker;
  res->rep = std::move(checker);
  return res;
}

void delete_ld_checker(ld_checker* checker) { delete checker; }

bool ld_checker_check(ld_checker* checker, int unhealthy_node_limit) {
  return checker->rep->check(unhealthy_node_limit);
}

} /* end extern "C" */
