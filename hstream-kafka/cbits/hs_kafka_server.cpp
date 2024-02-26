#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/redirect_error.hpp>
#include <asio/signal_set.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>
#include <iostream>
#include <list>
#include <thread>

#include "hs_kafka_server.h"

// ----------------------------------------------------------------------------

static void writeBE(int32_t value, uint8_t bytes[4]) {
  bytes[0] = (value >> 24) & 0xFF;
  bytes[1] = (value >> 16) & 0xFF;
  bytes[2] = (value >> 8) & 0xFF;
  bytes[3] = value & 0xFF;
}

static int32_t readBE(uint8_t bytes[4]) {
  return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
}

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

// Copy from asio example:
//
// https://think-async.com/Asio/asio-1.28.0/doc/asio/examples/cpp03_examples.html#asio.examples.cpp03_examples.http_server_2
//
// Using an io_context-per-CPU design.
//
// TODO: How about "using a single io_context and a thread pool calling
// io_context::run()" as this example does:
//
// https://think-async.com/Asio/asio-1.28.0/doc/asio/examples/cpp03_examples.html#asio.examples.cpp03_examples.http_server_3
class io_context_pool {
public:
  /// Construct the io_context pool.
  explicit io_context_pool(std::size_t pool_size) : next_io_context_(0) {
    if (pool_size == 0)
      throw std::runtime_error("io_context_pool size is 0");

    // Give all the io_contexts work to do so that their run() functions will
    // not exit until they are explicitly stopped.
    for (std::size_t i = 0; i < pool_size; ++i) {
      // FIXME: does it make sense to use concurrency_hint = 1?
      //
      // https://think-async.com/Asio/asio-1.28.0/doc/asio/overview/core/concurrency_hint.html
      io_context_ptr io_context(new asio::io_context(1));
      io_contexts_.push_back(io_context);
      work_.push_back(asio::make_work_guard(*io_context));
    }
  }

  /// Run all io_context objects in the pool.
  void run() {
    // Create a pool of threads to run all of the io_contexts.
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < io_contexts_.size(); ++i)
      threads.emplace_back([this, i] { io_contexts_[i]->run(); });

    // Wait for all threads in the pool to exit.
    for (std::size_t i = 0; i < threads.size(); ++i)
      threads[i].join();
  }

  /// Stop all io_context objects in the pool.
  void stop() {
    // Explicitly stop all io_contexts.
    for (std::size_t i = 0; i < io_contexts_.size(); ++i)
      io_contexts_[i]->stop();
  }

  /// Get an io_context to use.
  asio::io_context& get_io_context() {
    // Use a round-robin scheme to choose the next io_context to use.
    asio::io_context& io_context = *io_contexts_[next_io_context_];
    ++next_io_context_;
    if (next_io_context_ == io_contexts_.size())
      next_io_context_ = 0;
    return io_context;
  }

private:
  io_context_pool(const io_context_pool&) = delete;
  io_context_pool& operator=(const io_context_pool&) = delete;

  typedef std::shared_ptr<asio::io_context> io_context_ptr;
  typedef asio::executor_work_guard<asio::io_context::executor_type>
      io_context_work;

  /// The pool of io_contexts.
  std::vector<io_context_ptr> io_contexts_;

  /// The work that keeps the io_contexts running.
  std::list<io_context_work> work_;

  /// The next io_context to use for a connection.
  std::size_t next_io_context_;
};

// ----------------------------------------------------------------------------

class ServerHandler : public std::enable_shared_from_this<ServerHandler> {
public:
  ServerHandler(asio::ip::tcp::socket socket, HsCallback& callback,
                HsStablePtr& sp)
      : socket_(std::move(socket)), callback_(callback), sp_(sp) {}
  ServerHandler& operator=(const ServerHandler&) = delete;

  void start() {
    // There is no need to set no_delay as we focus on throughput
    //
    // asio::ip::tcp::no_delay no_delay(true);
    // socket_.set_option(no_delay);
    asio::co_spawn(
        socket_.get_executor(),
        [self = shared_from_this()] { return self->handler(); },
        asio::detached);
  }

  void stop() {
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both);
    socket_.close();
  }

  ~ServerHandler() {
    if (sp_ != nullptr) {
      hs_free_stable_ptr(sp_);
      // Make sure the haskell land does not use the freed stable pointer
      sp_ = nullptr;
    }
  }

private:
  asio::awaitable<void> handler() {
    try {
      uint8_t length_bytes_[4]; // Kafka protocol: big-endian. Both for request
                                // and response.
      asio::error_code ec;

      while (true) {
        co_await asio::async_read(
            socket_, asio::buffer(length_bytes_, 4),
            asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
          // Read failed, maybe client is done writing.
          co_return;
        }
        int32_t length_ = readBE(length_bytes_);
        // FIXME:
        // 1. How about the case when length_ is negative?
        // 2. How about using stack memory for small messages?
        std::vector<uint8_t> msg_bytes_(length_);
        co_await asio::async_read(socket_, asio::buffer(msg_bytes_),
                                  asio::use_awaitable);

        auto coro_lock = CoroLock(co_await asio::this_coro::executor, 1);
        server_request_t request{msg_bytes_.data(), msg_bytes_.size(),
                                 &coro_lock};
        server_response_t response;

        // Call haskell handler
        callback_(sp_, &request, &response);
        // Wait haskell handler done
        const auto [ec, _] = co_await coro_lock.async_receive(
            asio::as_tuple(asio::use_awaitable));

        if (socket_.is_open()) {
          if (response.data != nullptr) {
            // It's safe to use length_bytes_ again as we process the request
            // one by one.
            writeBE(response.data_size, length_bytes_);
            std::vector<asio::const_buffer> buffers;
            buffers.push_back(asio::buffer(length_bytes_, 4));
            buffers.push_back(asio::buffer(response.data, response.data_size));

            co_await asio::async_write(socket_, buffers, asio::use_awaitable);
            free(response.data);
          } else {
            // Server active close
            stop();
            co_return;
          }
        } else {
          // The socket is closed, stop the handler
          co_return;
        }
      }
    } catch (std::exception& e) {
      std::cerr << "Session exception: " << e.what() << std::endl;
      stop();
    }
  }

  asio::ip::tcp::socket socket_;
  HsStablePtr sp_;
  HsCallback& callback_;
};

asio::awaitable<void> listener(asio::ip::tcp::acceptor acceptor,
                               io_context_pool& context_pool,
                               HsCallback callback, HsNewStablePtr newConnCtx) {
  for (;;) {
    auto socket = co_await acceptor.async_accept(context_pool.get_io_context(),
                                                 asio::use_awaitable);
    auto peer_host = socket.remote_endpoint().address().to_string();
    conn_context_t conn_ctx{peer_host.data(), peer_host.size()};
    auto sp = newConnCtx(&conn_ctx);
    std::make_shared<ServerHandler>(std::move(socket), callback, sp)->start();
  }
}

// ----------------------------------------------------------------------------

extern "C" {

struct Server {
  io_context_pool io_context_pool_;

  explicit Server(std::size_t io_context_pool_size)
      : io_context_pool_(io_context_pool_size) {}
};

Server* new_kafka_server(std::size_t io_context_pool_size) {
  return new Server(io_context_pool_size);
}

void run_kafka_server(Server* server, const char* host, uint16_t port,
                      HsCallback callback, HsNewStablePtr newConnCtx,
                      int fd_on_started) {
  auto& context_pool = server->io_context_pool_;

  asio::ip::tcp::acceptor acceptor(context_pool.get_io_context());
  asio::ip::tcp::resolver resolver(acceptor.get_executor());
  asio::ip::tcp::endpoint endpoint =
      *resolver.resolve(std::string(host), std::to_string(port)).begin();
  free((void*)host);

  acceptor.open(endpoint.protocol());
  acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
  acceptor.bind(endpoint);
  acceptor.listen();

  asio::co_spawn(
      context_pool.get_io_context(),
      listener(std::move(acceptor), context_pool, callback, newConnCtx),
      asio::detached);

  // FIXME: Do we need to handle SIGINT and SIGTERM?
  //
  // asio::signal_set signals(io_context, SIGINT, SIGTERM);
  // signals.async_wait([&](auto, auto) { io_context.stop(); });

  hs_event_notify(fd_on_started);

  context_pool.run();
}

void stop_kafka_server(Server* server) { server->io_context_pool_.stop(); }

void delete_kafka_server(Server* server) { delete server; }

void ka_release_lock(CoroLock* channel, HsStablePtr mvar, HsInt cap,
                     HsInt* ret_code) {
  if (channel) {
    // NOTE: make sure the io_context(inside Server) is alive when we call
    // async_send, otherwise, you'll get a segfault.
    channel->async_send(asio::error_code{}, true,
                        [cap, mvar, ret_code](asio::error_code ec) {
                          *ret_code = (HsInt)ec.value();
                          hs_try_putmvar(cap, mvar);
                        });
  }
}

// ---
}
