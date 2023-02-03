#include "AdminClient.h"

#include <google/protobuf/empty.pb.h>
// TODO: Replace logdevice's debug library with hstream client's. So that we can
// remove the logdevice extra library in the cabal file.
#include <logdevice/common/debug.h>

using google::protobuf::Empty;

namespace hstream { namespace client {

folly::Optional<std::vector<ServerNode>>
AdminCommandClient::getNodesConfiguration(std::chrono::milliseconds timeout) {
  DescribeClusterResponse reply;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);
  grpc::Status status = stub_->DescribeCluster(&context, Empty(), &reply);

  if (status.ok()) {
    std::vector<ServerNode> nodesConfiguration;
    auto size = reply.servernodes_size();
    nodesConfiguration.reserve(size);
    for (auto& i : reply.servernodes()) {
      nodesConfiguration.push_back(i);
    }
    return nodesConfiguration;
  } else {
    ld_error("Run getNodesConfiguration error %d: %s", status.error_code(),
             status.error_message().c_str());
    return folly::none;
  }
}

folly::Future<AdminCommandClient::Response>
AdminCommandClient::sendAdminCommand(
    const AdminCommandClient::Request& req,
    std::shared_ptr<grpc::ChannelCredentials> cred,
    std::chrono::milliseconds timeout) {
  AdminCommandRequest request;
  request.set_command(req.request);

  ld_debug("Sending command to %s...", req.addr.c_str());

  AdminCommandResponse reply;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);

  auto stub = HStreamApi::NewStub(grpc::CreateChannel(req.addr, cred));
  grpc::Status status = stub->SendAdminCommand(&context, request, &reply);

  if (status.ok()) {
    return AdminCommandClient::Response(reply.result(), true, "");
  } else {
    return AdminCommandClient::Response("", false,
                                        std::to_string(status.error_code()) +
                                            ": " + status.error_message());
  }
}

std::vector<folly::SemiFuture<AdminCommandClient::Response>>
AdminCommandClient::asyncSend(
    const std::vector<AdminCommandClient::Request>& rs,
    std::chrono::milliseconds timeout) {
  std::vector<folly::SemiFuture<AdminCommandClient::Response>> futures;
  futures.reserve(rs.size());
  auto cred = cred_;

  for (auto& r : rs) {
    futures.push_back(
        folly::via(executor_.get()).then([r, cred, timeout](auto&&) mutable {
          return sendAdminCommand(r, cred, timeout);
        }));
  }

  return futures;
}

std::vector<AdminCommandClient::Response>
AdminCommandClient::send(const std::vector<AdminCommandClient::Request>& rs,
                         std::chrono::milliseconds timeout) {
  return folly::collectAll(AdminCommandClient::asyncSend(rs, timeout))
      .via(executor_.get())
      .thenValue(
          [](std::vector<folly::Try<AdminCommandClient::Response>> results) {
            std::vector<AdminCommandClient::Response> ret;
            ret.reserve(results.size());
            for (const auto& result : results) {
              if (result.hasValue()) {
                ret.emplace_back(result.value());
              } else {
                ret.emplace_back(std::string(), false,
                                 result.exception().what().toStdString());
              }
            }
            return ret;
          })
      .get();
}

}} // namespace hstream::client
