#include "hs_logdevice.h"
#include <boost/stacktrace.hpp>
#include <signal.h>

std::string* new_hs_std_string(std::string&& str) {
  auto value = new std::string;
  *value = str;
  return value;
}

// Copy string contents.
//
// Note that
// 1. this will not append the "\0" to the end of the memory.
// 2. you need to free the result manually.
char* copyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

extern "C" {

std::string* hs_cal_std_string_off(std::string* str, HsInt idx) {
  return str + idx;
}

void delete_vector_of_string(std::vector<std::string>* ss) { delete ss; }

void handle_fatal_signal(int signum) {
  ::signal(signum, SIG_DFL);
  std::cerr << "handle_fatal_signal(): Caught coredump signal " << signum
            << '\n';
  std::cerr << "Backtrace:\n" << boost::stacktrace::stacktrace() << '\n';
  ::raise(SIGTRAP);
}

void setup_sigsegv_handler() { ::signal(SIGSEGV, &handle_fatal_signal); }

// End
}
