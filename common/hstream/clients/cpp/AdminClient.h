#pragma once

#include <folly/Optional.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <grpcpp/grpcpp.h>
#include <string>

#include "HStream/Server/HStreamApi.grpc.pb.h"
#include "HStream/Server/HStreamApi.pb.h"

using namespace hstream::server;

namespace hstream { namespace client {

class AdminCommandClient {
public:
  explicit AdminCommandClient(std::string base_addr,
                              std::shared_ptr<grpc::ChannelCredentials> cred =
                                  grpc::InsecureChannelCredentials(),
                              size_t num_threads = 4)
      : stub_(HStreamApi::NewStub(grpc::CreateChannel(base_addr, cred))),
        cred_(cred),
        executor_(std::make_unique<folly::IOThreadPoolExecutor>(num_threads)) {}

  explicit AdminCommandClient(std::shared_ptr<grpc::Channel> channel,
                              std::shared_ptr<grpc::ChannelCredentials> cred =
                                  grpc::InsecureChannelCredentials(),
                              size_t num_threads = 4)
      : stub_(HStreamApi::NewStub(channel)), cred_(cred),
        executor_(std::make_unique<folly::IOThreadPoolExecutor>(num_threads)) {}

  struct Request {
    Request(std::string addr, std::string req) : addr(addr), request(req) {}
    std::string addr;
    std::string request;
  };

  struct Response {
    Response() {}
    Response(std::string response, bool success, std::string failure_reason)
        : response(response), success(success), failure_reason(failure_reason) {
    }
    std::string response{""};
    bool success{false};
    std::string failure_reason{""};
  };

  folly::Optional<std::vector<ServerNode>> getNodesConfiguration(
      std::chrono::milliseconds timeout = std::chrono::milliseconds{1000});

  static folly::Future<AdminCommandClient::Response> sendAdminCommand(
      const AdminCommandClient::Request& req,
      std::shared_ptr<grpc::ChannelCredentials> cred =
          grpc::InsecureChannelCredentials(),
      std::chrono::milliseconds timeout = std::chrono::milliseconds{1000});

  std::vector<Response>
  send(const std::vector<Request>& rs,
       std::chrono::milliseconds timeout = std::chrono::milliseconds{1000});

  std::vector<folly::SemiFuture<AdminCommandClient::Response>> asyncSend(
      const std::vector<Request>& rs,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{1000});

private:
  std::unique_ptr<HStreamApi::Stub> stub_;
  std::shared_ptr<grpc::ChannelCredentials> cred_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
};

}} // namespace hstream::client
