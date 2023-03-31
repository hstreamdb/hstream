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

// Explicitly instantiate
template char* copyString(const ld::Payload& payload);
template char* copyString(const std::string& payload);

template <typename T> char* copyString(const T& payload) {
  char* data_copy = nullptr;
  auto data_ = payload.data();
  auto size_ = payload.size();
  if (data_ && size_ > 0) {
    data_copy = reinterpret_cast<char*>(malloc(size_));
    if (!data_copy) {
      throw std::bad_alloc();
    }
    memcpy(data_copy, data_, size_);
  }
  return data_copy;
}

extern "C" {

std::string* hs_cal_std_string_off(std::string* str, HsInt idx) {
  return str + idx;
}

void delete_vector_of_cint(std::vector<int>* ss) { delete ss; }

HsInt get_vector_of_string_size(std::vector<std::string>* ss) {
  return ss->size();
}

std::string* get_vector_of_string_data(std::vector<std::string>* ss) {
  return ss->data();
}

// End
}
