#pragma once

#include <HsFFI.h>
#include <cstdint>

#include <folly/Unit.h>
#include <folly/futures/Promise.h>

struct server_request_t {
  uint8_t* data;
  size_t data_size;
  folly::Promise<folly::Unit>* lock;
};

struct server_response_t {
  // Must be initialized to nullptr. Because we'll use it to check if the
  // response is empty, and the haskell land may not set it to nullptr
  // (when the StablePtr == nullPtr).
  uint8_t* data = nullptr;
  size_t data_size;
};

struct conn_context_t {
  char* peer_host;
  size_t peer_host_size;
};

using HsCallback = void (*)(HsStablePtr, server_request_t*, server_response_t*);
using HsNewStablePtr = HsStablePtr (*)(conn_context_t*);
