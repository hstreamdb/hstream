#include "hs_logdevice.h"

using facebook::logdevice::logsconfig::Attribute;

extern "C" {
// ----------------------------------------------------------------------------

LogAttributes*
#define _ARG(ty, name) ty *name##_val, HsBool name##_inh
#define _MAYBE_ARG(ty, name)                                                   \
  HsBool name##_flag, ty *name##_val, HsBool name##_inh
poke_log_attributes(_ARG(int, replicationFactor), _ARG(int, syncedCopies),
                    _ARG(int, maxWritesInFlight), _ARG(bool, singleWriter),
                    _ARG(facebook::logdevice::NodeLocationScope,
                         syncReplicationScope),
                    _MAYBE_ARG(int, backlogDuration),
                    //
                    HsInt extras_len, StgArrBytes** keys, StgArrBytes** vals) {
#undef _ARG
  auto attrs = LogAttributes();
#define ADD_ATTR(x)                                                            \
  if (x##_val) {                                                               \
    attrs = attrs.with_##x(Attribute(*x##_val, x##_inh));                      \
  }
#define ADD_MAYBE_ATTR(name, t, f)                                             \
  if (name##_flag) {                                                           \
    if (name##_val) {                                                          \
      folly::Optional<t> val = f(*name##_val);                                 \
      attrs = attrs.with_##name(Attribute(val, name##_inh));                   \
    } else {                                                                   \
      attrs = attrs.with_##name(folly::none);                                  \
    }                                                                          \
  }
  ADD_ATTR(replicationFactor)
  ADD_ATTR(syncedCopies)
  ADD_ATTR(maxWritesInFlight)
  ADD_ATTR(singleWriter)
  ADD_ATTR(syncReplicationScope)
  ADD_MAYBE_ATTR(backlogDuration, std::chrono::seconds, std::chrono::seconds)
#undef ADD_ATTR
#undef ADD_MAYBE_ATTR

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

void peek_log_attributes(LogAttributes* attrs,
#define ARG(T, X) HsBool *X##_flag, T *X##_val, HsBool *X##_inh
#define ARG_MAYBE(T, X)                                                        \
  HsBool *X##_flag, HsBool *X##_val_flag, T *X##_val, HsBool *X##_inh
                         ARG(HsInt, replicationFactor),
                         ARG(HsInt, syncedCopies),
                         ARG(HsInt, maxWritesInFlight), ARG(bool, singleWriter),
                         ARG(facebook::logdevice::NodeLocationScope,
                             syncReplicationScope),
                         ARG_MAYBE(HsInt, backlogDuration))
#undef ARG
#undef ARG_MAYBE
{
#define PEEK(X)                                                                \
  *X##_flag = attrs->X().hasValue();                                           \
  *X##_inh = attrs->X().isInherited();                                         \
  if (*X##_flag) {                                                             \
    *X##_val = attrs->X().value();                                             \
  }
#define PEEK_MAYBE(X, VF)                                                      \
  *X##_flag = attrs->X().hasValue();                                           \
  *X##_inh = attrs->X().isInherited();                                         \
  if (*X##_flag) {                                                             \
    auto& val = attrs->X().value();                                            \
    *X##_val_flag = val.hasValue();                                            \
    if (*X##_val_flag) {                                                       \
      *X##_val = val.value() VF;                                               \
    }                                                                          \
  }
  PEEK(replicationFactor);
  PEEK(syncedCopies);
  PEEK(maxWritesInFlight);
  PEEK(singleWriter);
  PEEK(syncReplicationScope);
  PEEK_MAYBE(backlogDuration, .count());
#undef PEEK
#undef PEEK_MAYBE
}

void peek_log_attributes_extras(LogAttributes* attrs, size_t* len,
                                std::string** keys_ptr,
                                std::string** values_ptr,
                                std::vector<std::string>** keys_,
                                std::vector<std::string>** values_) {
  std::vector<std::string>* keys = new std::vector<std::string>;
  std::vector<std::string>* values = new std::vector<std::string>;

  if (attrs->extras().hasValue()) {
    auto& extras = attrs->extras().value();

    for (const auto& [key, value] : extras) {
      keys->push_back(key);
      values->push_back(value);
    }
  }

  *len = keys->size();
  *keys_ptr = keys->data();
  *values_ptr = values->data();
  *keys_ = keys;
  *values_ = values;
}

// ----------------------------------------------------------------------------
} // end extern "C"
