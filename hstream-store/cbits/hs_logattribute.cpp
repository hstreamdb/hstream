#include "hs_logdevice.h"

template <typename T>
using LogAttributeVal = facebook::logdevice::logsconfig::Attribute<T>;
using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;

extern "C" {
// ----------------------------------------------------------------------------

LogAttributes* new_log_attributes() { return new LogAttributes(); }
void free_log_attributes(LogAttributes* attrs) { delete attrs; }

// -------------------------------------
// Getter

// TODO
//const LogAttributeVal<int>& get_replicationFactor(LogAttributes* attrs) {
//  return attrs->replicationFactor();
//}

const char* describe_log_maxWritesInFlight(LogAttributes* attrs) {
  std::string desc = attrs->maxWritesInFlight().describe();
  return desc.c_str();
}

// -------------------------------------
// Setter

void with_replicationFactor(LogAttributes* attrs, int value) {
  LogAttributes attrs_ = attrs->with_replicationFactor(value);
  *attrs = attrs_;
}

// ----------------------------------------------------------------------------
} // end extern "C"
