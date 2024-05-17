#include "hs_logdevice.h"

using facebook::logdevice::logsconfig::Attribute;

extern "C" {
// ----------------------------------------------------------------------------

LogAttributes*
#define _ARG(ty, name) ty *name##_val, HsBool name##_inh
#define _MAYBE_ARG(ty, name)                                                   \
  HsBool name##_flag, ty *name##_val, HsBool name##_inh
#define _LIST_PAIR(name, ty_key, ty_val)                                       \
  HsInt name##_len, ty_key *name##_keys, ty_val *name##_vals, HsBool name##_inh
poke_log_attributes(_ARG(int, replicationFactor), _ARG(int, syncedCopies),
                    _ARG(int, maxWritesInFlight), _ARG(bool, singleWriter),
                    _ARG(facebook::logdevice::NodeLocationScope,
                         syncReplicationScope),
                    _LIST_PAIR(replicateAcross,
                               facebook::logdevice::NodeLocationScope, HsInt),
                    _MAYBE_ARG(int, backlogDuration), _ARG(bool, scdEnabled),
                    _ARG(bool, localScdEnabled), _ARG(bool, stickyCopySets),
                    //
                    HsInt extras_len, StgArrBytes** keys, StgArrBytes** vals) {
#undef _ARG
#undef _MAYBE_ARG
#undef _LIST_PAIR
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
  // replicateAcross
  if (replicateAcross_len > 0) {
    LogAttributes::ScopeReplicationFactors rs;
    rs.reserve(replicateAcross_len);
    for (int i = 0; i < replicateAcross_len; ++i) {
      rs.emplace_back(replicateAcross_keys[i], (int)replicateAcross_vals[i]);
    }
    attrs = attrs.with_replicateAcross(rs);
  }
  ADD_MAYBE_ATTR(backlogDuration, std::chrono::seconds, std::chrono::seconds)
  ADD_ATTR(scdEnabled)
  ADD_ATTR(localScdEnabled)
  ADD_ATTR(stickyCopySets)
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

void peek_log_attributes(
    LogAttributes* attrs,
#define ARG(T, X) HsBool *X##_flag, T *X##_val, HsBool *X##_inh
#define ARG_MAYBE(T, X)                                                        \
  HsBool *X##_flag, HsBool *X##_val_flag, T *X##_val, HsBool *X##_inh
#define ARG_LIST_PAIR(name, key_ty, val_ty)                                    \
  HsInt name##_len, key_ty *name##_keys, val_ty *name##_vals, HsBool *name##_inh
    ARG(HsInt, replicationFactor), ARG(HsInt, syncedCopies),
    ARG(HsInt, maxWritesInFlight), ARG(bool, singleWriter),
    ARG(facebook::logdevice::NodeLocationScope, syncReplicationScope),
    ARG_LIST_PAIR(replicateAcross, facebook::logdevice::NodeLocationScope,
                  HsInt),
    ARG_MAYBE(HsInt, backlogDuration), ARG(bool, scdEnabled),
    ARG(bool, localScdEnabled), ARG(bool, stickyCopySets))
#undef ARG
#undef ARG_MAYBE
#undef ARG_LIST_PAIR
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
#define PEEK_LIST_PAIR(name, key_ty, val_ty)                                   \
  *name##_inh = attrs->name().isInherited();                                   \
  if (name##_len > 0 && attrs->name().hasValue()) {                            \
    auto& val = attrs->name().value();                                         \
    for (int i = 0; i < name##_len; i++) {                                     \
      name##_keys[i] = val[i].first;                                           \
      name##_vals[i] = val[i].second;                                          \
    }                                                                          \
  }
  PEEK(replicationFactor);
  PEEK(syncedCopies);
  PEEK(maxWritesInFlight);
  PEEK(singleWriter);
  PEEK(syncReplicationScope);
  PEEK_LIST_PAIR(replicateAcross, facebook::logdevice::NodeLocationScope,
                 HsInt);
  PEEK_MAYBE(backlogDuration, .count());
  PEEK(scdEnabled);
  PEEK(localScdEnabled);
  PEEK(stickyCopySets);
#undef PEEK
#undef PEEK_MAYBE
#undef PEEK_LIST_PAIR
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

/* negative len means nothing */
HsInt get_replicateAcross_size(LogAttributes* attrs) {
  if (attrs->replicateAcross().hasValue()) {
    return attrs->replicateAcross().value().size();
  } else {
    return -1;
  }
}

// ----------------------------------------------------------------------------
} // end extern "C"
