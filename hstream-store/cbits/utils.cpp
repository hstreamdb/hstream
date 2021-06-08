#include "hs_logdevice.h"

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

// End
}
