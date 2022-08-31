#include <iostream>
#include <memory>
#include <semaphore.h>
#include <string>
#include <thread>

#include <boost/program_options.hpp>
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <logdevice/include/Client.h>
#include <logdevice/include/ClientSettings.h>

#include "HStream/Server/HStreamApi.grpc.pb.h"
#include "HStream/Server/HStreamApi.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

namespace hs = hstream::server;
namespace ld = facebook::logdevice;

// ----------------------------------------------------------------------------

enum CallStatus { CREATE, PROCESS, FINISH };

struct HStreamCtx {
  std::string host;
  int port;
  std::shared_ptr<ld::Client>& ld_client;
};

// ----------------------------------------------------------------------------

class RpcCallAbc {
public:
  RpcCallAbc(){};
  virtual ~RpcCallAbc(){};
  virtual void proceed() noexcept {
    printf("proceed must be overridden by subclasses");
    assert(false);
  };
};

template <class DerivedType, class RequestType, class ResponseType>
class RpcCallBase : public RpcCallAbc {
public:
  typedef RpcCallBase<DerivedType, RequestType, ResponseType> cls;

  RpcCallBase(hs::HStreamApi::AsyncService* service, ServerCompletionQueue* cq,
              HStreamCtx& hstream_ctx)
      : service_(service), cq_(cq), hstream_ctx_(hstream_ctx),
        responder_(&ctx_) {
    proceed();
  };

  void proceed() noexcept override {
    if (status_ == CallStatus::CREATE) {
      status_ = CallStatus::PROCESS;
      static_cast<DerivedType*>(this)->request_rpc_method();
    } else if (status_ == CallStatus::PROCESS) {
      new cls(service_, cq_, hstream_ctx_);
      grpc::Status ret = static_cast<DerivedType*>(this)->handler();
      status_ = CallStatus::FINISH;
      responder_.Finish(reply_, ret, this);
    } else {
      GPR_ASSERT(status_ == CallStatus::FINISH);
      delete this;
    }
  }

protected:
  hs::HStreamApi::AsyncService* service_;
  ServerCompletionQueue* cq_;
  ServerContext ctx_;
  CallStatus status_ = CallStatus::CREATE; // The current serving state.
  HStreamCtx& hstream_ctx_;

  RequestType request_;
  ResponseType reply_;
  ServerAsyncResponseWriter<ResponseType> responder_;
};

// ----------------------------------------------------------------------------
// Handlers

#define SIMPLE_RPC_CALL(Name, Req, Resp)                                       \
  class Name##Call : public RpcCallBase<Name##Call, Req, Resp> {               \
  public:                                                                      \
    Name##Call(hs::HStreamApi::AsyncService* service,                          \
               ServerCompletionQueue* cq, HStreamCtx& hstream_ctx)             \
        : RpcCallBase(service, cq, hstream_ctx) {}                             \
                                                                               \
    void request_rpc_method() {                                                \
      service_->Request##Name(&ctx_, &request_, &responder_, cq_, cq_, this);  \
    }                                                                          \
                                                                               \
    grpc::Status handler();                                                    \
  };

SIMPLE_RPC_CALL(Echo, hs::EchoRequest, hs::EchoResponse)
grpc::Status EchoCall::handler() {
  *reply_.mutable_msg() = std::move(*request_.mutable_msg());
  return Status::OK;
}

SIMPLE_RPC_CALL(DescribeCluster, google::protobuf::Empty,
                hs::DescribeClusterResponse)
grpc::Status DescribeClusterCall::handler() {
  reply_.set_protocolversion("0.0.0");
  reply_.set_serverversion("0.0.0");
  auto node = reply_.add_servernodes();
  node->set_id(1000);
  node->set_host(hstream_ctx_.host);
  node->set_port(hstream_ctx_.port);

  return Status::OK;
}

SIMPLE_RPC_CALL(ListShards, hs::ListShardsRequest, hs::ListShardsResponse)
grpc::Status ListShardsCall::handler() {
  auto dir_path = "/hstream/stream/" + request_.streamname();
  auto dir = hstream_ctx_.ld_client->getDirectorySync(dir_path);
  if (!dir)
    throw std::runtime_error("getDirectory failed: " + dir_path);
  auto& logsMap = dir->logs();

  for (auto& [_groupkey, log] : logsMap) {
    auto shard = reply_.add_shards();

    ld::logid_t logid = log->range().first;
    shard->set_streamname(request_.streamname());
    shard->set_shardid(logid.val());

    if (log->attrs().extras().hasValue()) {
      auto& extras = log->attrs().extras().value();
      auto startKey = extras.find("startKey");
      auto endKey = extras.find("endKey");
      auto epoch = extras.find("epoch");
      if (startKey == extras.end() || endKey == extras.end() ||
          epoch == extras.end()) {
        throw std::runtime_error("Empty attrs");
      }
      shard->set_starthashrangekey(startKey->second);
      shard->set_endhashrangekey(endKey->second);
      shard->set_epoch(std::stoull(epoch->second));
    } else {
      // TODO
      throw std::runtime_error("Empty extras");
    }
  }

  return Status::OK;
}

SIMPLE_RPC_CALL(LookupShard, hs::LookupShardRequest, hs::LookupShardResponse)
grpc::Status LookupShardCall::handler() {
  reply_.set_shardid(request_.shardid());
  auto node = reply_.mutable_servernode();
  node->set_id(1000);
  node->set_host(hstream_ctx_.host);
  node->set_port(hstream_ctx_.port);

  return Status::OK;
}

SIMPLE_RPC_CALL(CreateStream, hs::Stream, hs::Stream)
grpc::Status CreateStreamCall::handler() {
  // TODO
  // auto dir_path = "/hstream/stream" + request_.streamname();
  // auto dir_attrs = ld::client::LogAttributes()
  //                      .with_replicationFactor(request_.replicationfactor())
  //                      .with_backlogDuration(
  //                          std::chrono::seconds(request_.backlogduration()));
  // auto dir =
  //     hstream_ctx_.ld_client->makeDirectorySync(dir_path, false, dir_attrs);
  // if (!dir)
  //   throw std::runtime_error("makeDirectory failed!");

  return Status::OK;
}

SIMPLE_RPC_CALL(Append, hs::AppendRequest, hs::AppendResponse)
grpc::Status AppendCall::handler() {
  auto streamName = request_.streamname();
  auto logid = request_.shardid();
  auto batchSize = request_.records().batchsize();
  auto payload = request_.records().payload();

  // The "payloads" is a fixed length c-like array of string
  char* c_payload = (char*)payload.c_str();
  int c_payload_len = payload.size();
  int c_payloads_total_len = 1;

  ld::lsn_t lsn;

  sem_t sem;
  sem_init(&sem, 0, 0);

  ld::append_callback_t append_cb = [&](ld::Status st,
                                        const ld::DataRecord& record) {
    if (st == ld::E::OK) {
      lsn = record.attrs.lsn;
      sem_post(&sem);
    } else {
      printf("error: append failed\n");
    }
  };
  int ret = hstream_ctx_.ld_client->appendBatched(
      ld::logid_t(logid), &c_payload, &c_payload_len, c_payloads_total_len,
      append_cb);
  if (ret != 0) {
    fprintf(stderr, "error: failed to post append: %s\n",
            ld::error_description(ld::err));
    sem_post(&sem);
  }

  sem_wait(&sem);

  reply_.set_streamname(streamName);
  reply_.set_shardid(logid);

  for (unsigned int i = 0; i < batchSize; i++) {
    auto recordid = reply_.add_recordids();
    recordid->set_shardid(logid);
    recordid->set_batchid(lsn);
    recordid->set_batchindex(i);
  }

  return Status::OK;
}

// ----------------------------------------------------------------------------

class ServerImpl final {
public:
  ServerImpl(HStreamCtx& hstream_ctx) : hstream_ctx_(hstream_ctx) {}

  void run() {
    std::string server_address(hstream_ctx_.host + ":" +
                               std::to_string(hstream_ctx_.port));

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    const auto parallelism = std::thread::hardware_concurrency();
    for (unsigned int i = 0; i < parallelism; i++) {
      cq_.emplace_back(builder.AddCompletionQueue());
    }

    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    for (unsigned int i = 0; i < parallelism; i++) {
      server_threads_.emplace_back(
          std::thread([this, i] { this->HandleRpcs(i); }));
    }

    for (auto& thread : server_threads_) {
      thread.join();
    }
  }

  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (auto& cq : cq_)
      cq->Shutdown();
  }

private:
  // This can be run in multiple threads if needed.
  void HandleRpcs(int i) {
    new EchoCall(&service_, cq_[i].get(), hstream_ctx_);
    new DescribeClusterCall(&service_, cq_[i].get(), hstream_ctx_);
    new ListShardsCall(&service_, cq_[i].get(), hstream_ctx_);
    new LookupShardCall(&service_, cq_[i].get(), hstream_ctx_);
    new CreateStreamCall(&service_, cq_[i].get(), hstream_ctx_);
    new AppendCall(&service_, cq_[i].get(), hstream_ctx_);

    void* tag; // uniquely identifies a request.
    bool ok;
    while (true) {
      GPR_ASSERT(cq_[i]->Next(&tag, &ok));
      RpcCallAbc* call = static_cast<RpcCallAbc*>(tag);
      if (!ok) {
        delete call;
        continue;
      }
      call->proceed();
    }
  }

  HStreamCtx& hstream_ctx_;

  std::vector<std::unique_ptr<ServerCompletionQueue>> cq_;
  hs::HStreamApi::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::vector<std::thread> server_threads_;
};

// ----------------------------------------------------------------------------

struct {
  std::string host;
  int port;
  std::string store_config;
} cli_options;

void parse_command_line(int argc, const char** argv) {
  using boost::program_options::value;
  namespace style = boost::program_options::command_line_style;
  try {
    boost::program_options::options_description desc("Options");

    // clang-format off
    desc.add_options()
      ("help,h", "print help and exit")
      ("host", value<std::string>(&cli_options.host)->default_value("0.0.0.0"),
       "server host")
      ("port", value<int>(&cli_options.port)->default_value(7570),
       "server port")
      ("store_config,c",
        value<std::string>(&cli_options.store_config)->default_value(
          "./local-data/logdevice/logdevice.conf"),
        "location of the store config.")
     ;
    // clang-format on

    boost::program_options::positional_options_description positional;
    positional.add("store_config", /* max_count */ 1);
    boost::program_options::command_line_parser parser(argc, argv);
    boost::program_options::variables_map parsed;
    boost::program_options::store(
        parser.options(desc)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        parsed);
    if (parsed.count("help")) {
      std::cout << "A fake cpp server" << '\n' << desc;
      exit(0);
    }
    boost::program_options::notify(parsed);

  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}

int main(int argc, const char** argv) {
  parse_command_line(argc, argv);

  std::shared_ptr<ld::Client> client =
      ld::ClientFactory().create(cli_options.store_config);
  HStreamCtx hstream_ctx{cli_options.host, cli_options.port, client};

  ServerImpl server(hstream_ctx);
  server.run();

  return 0;
}
