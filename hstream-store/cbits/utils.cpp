#include "hs_logdevice.h"

std::string* new_hs_std_string(std::string&& str) {
  auto value = new std::string;
  *value = str;
  return value;
}

char* copyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

extern "C" {

std::string* hs_cal_std_string_off(std::string* str, HsInt idx) {
  return str + idx;
}
}
