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

#include "hs_kafka_server.h"

static void writeBE(int32_t value, uint8_t bytes[4]) {
  bytes[0] = (value >> 24) & 0xFF;
  bytes[1] = (value >> 16) & 0xFF;
  bytes[2] = (value >> 8) & 0xFF;
  bytes[3] = value & 0xFF;
}

static int32_t readBE(uint8_t bytes[4]) {
  return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
}

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

asio::awaitable<void> listener(asio::ip::tcp::acceptor acceptor,
                               HsCallback callback, HsNewStablePtr newConnCtx) {
  for (;;) {
    auto socket = co_await acceptor.async_accept(asio::use_awaitable);
    auto peer_host = socket.remote_endpoint().address().to_string();
    conn_context_t conn_ctx{peer_host.data(), peer_host.size()};
    auto sp = newConnCtx(&conn_ctx);
    std::make_shared<ServerHandler>(std::move(socket), callback, sp)->start();
  }
}

// ----------------------------------------------------------------------------

extern "C" {

struct Server {
  asio::io_context io_context{1};
};

Server* new_kafka_server() { return new Server(); }

void run_kafka_server(Server* server, const char* host, uint16_t port,
                      HsCallback callback, HsNewStablePtr newConnCtx,
                      int fd_on_started) {
  // Create an address from an IPv4 address string in dotted decimal form, or
  // from an IPv6 address in hexadecimal notation.
  //
  // FIXME: what if the host is a domain? e.g. 'localhost'
  auto addr = asio::ip::make_address(host);
  free((void*)host);
  auto& io_context = server->io_context;

  asio::co_spawn(io_context,
                 listener(asio::ip::tcp::acceptor(io_context, {addr, port},
                                                  true /*reuse_addr*/),
                          callback, newConnCtx),
                 asio::detached);

  // FIXME: Do we need to handle SIGINT and SIGTERM?
  //
  // asio::signal_set signals(io_context, SIGINT, SIGTERM);
  // signals.async_wait([&](auto, auto) { io_context.stop(); });

  hs_event_notify(fd_on_started);

  io_context.run();
}

void stop_kafka_server(Server* server) { server->io_context.stop(); }

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
