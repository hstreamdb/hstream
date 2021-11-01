#pragma once

#include <exception>
#include <folly/Likely.h>
#include <stdexcept>

namespace hstream { namespace common { namespace dbg {

/**
 * Terminate cpp runtime, should be catched by haskell.
 */
struct ToHsException : public std::runtime_error {
  ToHsException(const char* msg) : std::runtime_error(msg) {}
  ToHsException() : std::runtime_error("Some errors happend in cpp runtime") {}
};

#define hs_check(expr, msg...)                                                 \
  do {                                                                         \
    if (UNLIKELY(!(expr))) {                                                   \
      auto errmsg = std::string(#msg);                                         \
      if (!errmsg.empty()) {                                                    \
        throw hstream::common::dbg::ToHsException(errmsg.c_str());             \
      } else {                                                                 \
        ld_error("hs_check failed: %s", #expr);                                   \
        throw hstream::common::dbg::ToHsException();                           \
      }                                                                        \
    }                                                                          \
  } while (false)

}}} // namespace hstream::common::dbg
