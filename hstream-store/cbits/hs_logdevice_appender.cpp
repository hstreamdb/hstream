#include "hs_logdevice.h"
#include "utils.h"

using facebook::logdevice::KeyType;

facebook::logdevice::Status _append_sync(logdevice_client_t* client,
                                         facebook::logdevice::logid_t logid,
                                         std::string&& payload,
                                         AppendAttributes&& attrs, int64_t* ts,
                                         c_lsn_t* lsn_ret) {
  c_lsn_t result;
  if (ts) {
    std::chrono::milliseconds timestamp;
    result = client->rep->appendSync(logid, payload, attrs, &timestamp);
    *ts = timestamp.count();
  } else {
    result = client->rep->appendSync(facebook::logdevice::logid_t(logid),
                                     payload, attrs, nullptr);
  }

  if (result == facebook::logdevice::LSN_INVALID) {
    return facebook::logdevice::err;
  }
  *lsn_ret = result;
  return facebook::logdevice::E::OK;
}

extern "C" {
// ----------------------------------------------------------------------------

void ld_cons_append_attributes(AppendAttributes* attrs, KeyType* keytypes,
                               const char** keyvals, HsInt optional_keys_len,
                               uint8_t* counter_key, int64_t* counter_val,
                               HsInt counters_len) {
  std::map<KeyType, std::string> optional_keys;
  if (optional_keys_len > 0) {
    for (int i = 0; i < optional_keys_len; ++i) {
      optional_keys[keytypes[i]] = keyvals[i];
    }
  }

  folly::Optional<std::map<uint8_t, int64_t>> counters = folly::none;
  if (counters_len >= 0) {
    std::map<uint8_t, int64_t> counters_map;
    for (int i = 0; i < counters_len; ++i) {
      counters_map[counter_key[i]] = counter_val[i];
    }
    counters = counters_map;
  }

  attrs->optional_keys = optional_keys;
  attrs->counters = counters;
}

AppendAttributes*
ld_new_append_attributes(KeyType* keytypes, const char** keyvals,
                         HsInt optional_keys_len, uint8_t* counter_key,
                         int64_t* counter_val, HsInt counters_len) {
  AppendAttributes* attrs = new AppendAttributes;
  ld_cons_append_attributes(attrs, keytypes, keyvals, optional_keys_len,
                            counter_key, counter_val, counters_len);
  return attrs;
}

void ld_free_append_attributes(AppendAttributes* attrs) { delete attrs; }

facebook::logdevice::Status
logdevice_append_sync(logdevice_client_t* client, c_logid_t logid,
                      const char* payload, HsInt offset,
                      HsInt length, // payload
                      int64_t* ts, c_lsn_t* lsn_ret) {
  std::string user_payload(payload + offset, length);
  return _append_sync(client, facebook::logdevice::logid_t(logid),
                      std::move(user_payload), AppendAttributes(), ts, lsn_ret);
}

facebook::logdevice::Status
logdevice_append_with_attrs_sync(logdevice_client_t* client, c_logid_t logid,
                                 const char* payload, HsInt offset,
                                 HsInt length, // payload
                                 KeyType keytype,
                                 const char* keyval, // optional_key
                                 int64_t* ts, c_lsn_t* lsn_ret) {
  AppendAttributes attrs;
  attrs.optional_keys[keytype] = keyval;
  std::string user_payload(payload + offset, length);
  return _append_sync(client, facebook::logdevice::logid_t(logid),
                      std::move(user_payload), std::move(attrs), ts, lsn_ret);
}

// ----------------------------------------------------------------------------
} // end extern "C"
