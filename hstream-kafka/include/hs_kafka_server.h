#pragma once

#include <HsFFI.h>
#include <cstdint>

struct server_request_t {
  uint8_t* data;
  size_t data_size;
};

struct server_response_t {
  uint8_t* data;
  size_t data_size;
};

struct conn_context_t {
  char* peer_host;
  size_t peer_host_size;
};

using HsCallback = void (*)(HsStablePtr, server_request_t*, server_response_t*);
using HsNewStablePtr = HsStablePtr (*)(conn_context_t*);
