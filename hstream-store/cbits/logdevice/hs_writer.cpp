#include "hs_logdevice.h"

facebook::logdevice::Status
_append_payload_sync(logdevice_client_t* client,
                     facebook::logdevice::logid_t logid,
                     // Payload
                     const char* payload, HsInt offset, HsInt length,
                     // Payload End
                     AppendAttributes&& attrs, int64_t* ts, c_lsn_t* lsn_ret) {
  c_lsn_t result;
  if (ts) {
    std::chrono::milliseconds timestamp;
    result = client->rep->appendSync(logid, Payload(payload + offset, length),
                                     attrs, &timestamp);
    *ts = timestamp.count();
  } else {
    result = client->rep->appendSync(facebook::logdevice::logid_t(logid),
                                     Payload(payload + offset, length), attrs,
                                     nullptr);
  }

  if (result == facebook::logdevice::LSN_INVALID) {
    return facebook::logdevice::err;
  }
  *lsn_ret = result;
  return facebook::logdevice::E::OK;
}

facebook::logdevice::Status _append_payload_async(
    HsStablePtr mvar, HsInt cap, logdevice_append_cb_data_t* cb_data,
    logdevice_client_t* client, facebook::logdevice::logid_t logid,
    // Payload
    const char* payload, HsInt offset, HsInt length,
    // Payload End
    AppendAttributes&& attrs) {
  auto cb = [cb_data, mvar, cap](facebook::logdevice::Status st,
                                 const DataRecord& r) {
    if (cb_data) {
      cb_data->st = static_cast<c_error_code_t>(st);
      cb_data->logid = r.logid.val_;
      cb_data->lsn = r.attrs.lsn;
      cb_data->timestamp = r.attrs.timestamp.count();
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->append(logid, Payload(payload + offset, length),
                                std::move(cb), attrs);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
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
                      // Payload
                      const char* payload, HsInt offset, HsInt length,
                      // Payload End
                      c_timestamp_t* ts, c_lsn_t* lsn_ret) {
  return _append_payload_sync(client, logid_t(logid), payload, offset, length,
                              AppendAttributes(), ts, lsn_ret);
}

facebook::logdevice::Status
logdevice_append_with_attrs_sync(logdevice_client_t* client, c_logid_t logid,
                                 // Payload
                                 const char* payload, HsInt offset,
                                 HsInt length,
                                 // Payload End
                                 // optional_key
                                 KeyType keytype, const char* keyval,
                                 // OptionalKey End
                                 c_timestamp_t* ts, c_lsn_t* lsn_ret) {
  AppendAttributes attrs;
  attrs.optional_keys[keytype] = keyval;
  return _append_payload_sync(client, logid_t(logid), payload, offset, length,
                              std::move(attrs), ts, lsn_ret);
}

facebook::logdevice::Status
logdevice_append_async(HsStablePtr mvar, HsInt cap,
                       logdevice_append_cb_data_t* cb_data,
                       logdevice_client_t* client, c_logid_t logid,
                       const char* payload, HsInt offset, HsInt length) {
  return _append_payload_async(mvar, cap, cb_data, client, logid_t(logid),
                               payload, offset, length, AppendAttributes());
}

facebook::logdevice::Status logdevice_append_with_attrs_async(
    HsStablePtr mvar, HsInt cap, logdevice_append_cb_data_t* cb_data,
    logdevice_client_t* client, c_logid_t logid, const char* payload,
    HsInt offset, HsInt length, KeyType keytype, const char* keyval) {
  AppendAttributes attrs;
  attrs.optional_keys[keytype] = keyval;
  return _append_payload_async(mvar, cap, cb_data, client, logid_t(logid),
                               payload, offset, length, std::move(attrs));
}

// ----------------------------------------------------------------------------
} // end extern "C"
