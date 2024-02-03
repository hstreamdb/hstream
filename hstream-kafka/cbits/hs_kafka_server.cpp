#include <HsFFI.h>

#include <folly/SocketAddress.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <wangle/bootstrap/ServerBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/channel/Handler.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/service/ExecutorFilter.h>
#include <wangle/service/ServerDispatcher.h>
#include <wangle/service/Service.h>

#include "hs_kafka_server.h"

using namespace wangle;

// Note: this should be the end of pipeline
class ServerHandler : public HandlerAdapter<std::unique_ptr<folly::IOBuf>> {
public:
  explicit ServerHandler(HsCallback& callback, HsStablePtr& sp,
                         std::shared_ptr<folly::Executor> exe)
      : callback_(callback), sp_(sp), exe_(exe) {}

  void read(Context* ctx, std::unique_ptr<folly::IOBuf> req) override {
    folly::fbstring request_ = req->moveToFbString();

    folly::Promise<folly::Unit> lock;
    folly::Future<folly::Unit> lock_fu = lock.getSemiFuture().via(exe_.get());

    server_request_t hs_req{(uint8_t*)request_.data(), request_.size(), &lock};
    server_response_t hs_resp;

    callback_(sp_, &hs_req, &hs_resp);

    std::move(lock_fu).get();

    if (hs_resp.data != nullptr) {
      std::unique_ptr<folly::IOBuf> resp =
          folly::IOBuf::takeOwnership(hs_resp.data, hs_resp.data_size);
      ctx->fireWrite(std::move(resp));
    } else {
      ctx->fireClose();
    }
  }

  // [?] Do not need to call ctx->fireReadEOF() here, since this is the last
  // handler in pipeline.
  void readEOF(Context* ctx) override { onClientExit(); }

  // [?] Do not need to call ctx->fireReadException() here, since this is the
  // last handler in pipeline.
  void readException(Context* ctx, folly::exception_wrapper e) override {
    onClientExit();
  }

private:
  HsCallback& callback_;
  HsStablePtr sp_;
  std::shared_ptr<folly::Executor> exe_;

  void onClientExit() {
    // FIXME: lock guard here?
    if (sp_ != nullptr) {
      hs_free_stable_ptr(sp_);
      // Make sure the haskell land does not use the freed stable pointer
      sp_ = nullptr;
    }
  }
};

class RpcPipelineFactory : public PipelineFactory<DefaultPipeline> {
public:
  explicit RpcPipelineFactory(HsCallback callback, HsNewStablePtr newConnCtx,
                              std::shared_ptr<folly::Executor> exe)
      : callback_(callback), newConnCtx_(newConnCtx), exe_(exe) {}

  DefaultPipeline::Ptr
  newPipeline(std::shared_ptr<folly::AsyncTransport> sock) override {
    auto peer_host = sock->getPeerAddress().getHostStr();
    conn_context_t conn_ctx{peer_host.data(), peer_host.size()};
    auto hssp = newConnCtx_(&conn_ctx);

    auto pipeline = DefaultPipeline::create();
    pipeline->addBack(AsyncSocketHandler(sock));
    // ensure we can write from any thread
    pipeline->addBack(EventBaseHandler());
    pipeline->addBack(LengthFieldBasedFrameDecoder());
    pipeline->addBack(LengthFieldPrepender());
    // Haskell handler
    pipeline->addBack(ServerHandler(callback_, hssp, exe_));
    pipeline->finalize();
    return pipeline;
  }

private:
  HsCallback callback_;
  HsNewStablePtr newConnCtx_;
  std::shared_ptr<folly::Executor> exe_;
};

void hs_event_notify(int& fd) {
  if (fd == -1)
    return;

  uint64_t u = 1;
  ssize_t s = write(fd, &u, sizeof(uint64_t));
  if (s != sizeof(uint64_t)) {
    fprintf(stderr, "write to fd %d failed!", fd);
    return;
  }
}

// ----------------------------------------------------------------------------

extern "C" {

ServerBootstrap<DefaultPipeline>* new_kafka_server() {
  return new ServerBootstrap<DefaultPipeline>();
}

void run_kafka_server(ServerBootstrap<DefaultPipeline>* server,
                      const char* host, uint16_t port, HsCallback callback,
                      HsNewStablePtr newConnCtx, int fd_on_started) {
  auto addr = folly::SocketAddress{host, port};
  free((void*)host);

  auto threads = std::thread::hardware_concurrency();
  if (threads <= 0) {
    // Reasonable mid-point for concurrency when actual value unknown
    threads = 8;
  }
  auto io_group = std::make_shared<folly::IOThreadPoolExecutor>(
      threads, std::make_shared<folly::NamedThreadFactory>("IO Thread"));

  server->childPipeline(
      std::make_shared<RpcPipelineFactory>(callback, newConnCtx, io_group));

  // Using default accept_group (a single thread)
  server->group(io_group);

  server->bind(addr);

  hs_event_notify(fd_on_started);
  server->waitForStop();

  // Server stopped
  delete server;
}

void stop_kafka_server(ServerBootstrap<DefaultPipeline>* server) {
  server->stop();
}

void release_lock(folly::Promise<folly::Unit>* p) {
  p->setValue(folly::Unit{});
}

// ---
}
