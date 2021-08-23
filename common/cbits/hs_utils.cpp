#include "hs_common.h"

#include <boost/stacktrace.hpp>
#include <signal.h>

extern "C" {
// ----------------------------------------------------------------------------

void handle_fatal_signal(int signum) {
  ::signal(signum, SIG_DFL);
  std::cerr << "handle_fatal_signal(): Caught coredump signal " << signum
            << '\n';
  std::cerr << "Backtrace:\n" << boost::stacktrace::stacktrace() << '\n';
  ::raise(SIGTRAP);
}

void setup_sigsegv_handler() { ::signal(SIGSEGV, &handle_fatal_signal); }

// ----------------------------------------------------------------------------
}
