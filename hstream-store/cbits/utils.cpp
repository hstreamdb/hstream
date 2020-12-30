#include "hs_logdevice.h"
#include "utils.h"

char *copyString(const std::string &str) {
  char *result = reinterpret_cast<char *>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}
