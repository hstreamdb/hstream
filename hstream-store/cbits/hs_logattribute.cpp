#include "hs_logdevice.h"

extern "C" {
// ----------------------------------------------------------------------------

// TODO
#if __GLASGOW_HASKELL__ < 810
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgMutArrPtrs* keys_, StgMutArrPtrs* vals_) {
  StgArrBytes** keys = (StgArrBytes**)keys_->payload;
  StgArrBytes** vals = (StgArrBytes**)vals_->payload;
#else
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgArrBytes** keys, StgArrBytes** vals) {
#endif
  auto attrs = LogAttributes().with_replicationFactor(replicationFactor);
  if (extras_len > 0) {
    LogAttributes::ExtrasMap extras;
    for (int i = 0; i < extras_len; ++i) {
      extras[(char*)(keys[i]->payload)] = (char*)(vals[i]->payload);
    }
    attrs = attrs.with_extras(extras);
  }
  LogAttributes* attrs_ptr = new LogAttributes(attrs);
  return attrs_ptr;
}

void free_log_attributes(LogAttributes* attrs) { delete attrs; }

bool exist_log_attrs_extras(LogAttributes* attrs, char* key) {
  if (attrs) {
    auto extras = attrs->extras();
    return extras.hasValue() && extras.value().contains(key);
  }
  return false;
}

// get extras without check the key exists.
std::string* get_log_attrs_extra(LogAttributes* attrs, char* key) {
  auto extras = attrs->extras().value();
  return new_hs_std_string(std::move(extras[key]));
}

void set_log_attrs_extra(LogAttributes* attrs, char* key, char* val) {
  auto extras = attrs->extras().value();
  extras[key] = val;
}

// TODO: macro
int get_replicationFactor(LogAttributes* attrs) {
  return attrs->replicationFactor().value();
}

std::string* describe_log_maxWritesInFlight(LogAttributes* attrs) {
  return new_hs_std_string(attrs->maxWritesInFlight().describe());
}

// ----------------------------------------------------------------------------
} // end extern "C"
