#include "hs_logdevice.h"

using facebook::logdevice::Status;

extern "C" {
// ----------------------------------------------------------------------------

// Function that VersionedConfigStore could call on stored values to
// extract the corresponding membership version. If value is invalid, the
// function should return folly::none.
//
// This function should be synchronous, relatively fast, and should not
// consume the value string (folly::StringPiece does not take ownership of the
// string).
//
// FIXME
folly::Optional<VersionedConfigStore::version_t>
extract_function(folly::StringPiece s) {
  int i = 0;
  for (i; i < s.size(); ++i) {
    if (s[i] == ':') {
      break;
    }
  }
  u_int64_t version = 0;
  try {
    std::string ver_str = (s.subpiece(0, i).str());
    version = std::stoll(ver_str);
  } catch (std::exception& e) {
    std::cout << "-> Extract version error: " << e.what() << "\n";
  }
  if (version) {
    return VersionedConfigStore::version_t(version);
  }
  return folly::none;
}

logdevice_vcs_t* new_rsm_based_vcs(logdevice_client_t* client, c_logid_t logid,
                                   int64_t stop_timeout) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client->rep.get());
  ld_check(client_impl);
  auto vcs_ = std::make_unique<RSMBasedVersionedConfigStore>(
      logid_t(logid), extract_function, &(client_impl->getProcessor()),
      std::chrono::milliseconds(stop_timeout));
  logdevice_vcs_t* vcs = new logdevice_vcs_t;
  vcs->rep = std::move(vcs_);
  return vcs;
}

void free_logdevice_vcs(logdevice_vcs_t* vcs) { delete vcs; }

void logdevice_vcs_get_config(logdevice_vcs_t* vcs, const char* key,
                              c_vcs_config_version_t* base_version_,
                              HsStablePtr mvar, HsInt cap,
                              vcs_value_callback_data_t* cb_data) {
  auto value_cb = [mvar, cap, cb_data](facebook::logdevice::Status st,
                                       std::string val) {
    if (cb_data) {
      cb_data->st = static_cast<c_error_code_t>(st);
      // If status is OK, cb will be invoked with the value.
      // Otherwise, the value parameter is meaningless (but
      // default-constructed).
      if (st == facebook::logdevice::Status::OK) {
        cb_data->val_len = val.size();
        cb_data->value = copyString(val);
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };

  if (base_version_) {
    vcs->rep->getConfig(std::string(key), value_cb,
                        VersionedConfigStore::version_t(*base_version_));
  } else {
    vcs->rep->getConfig(std::string(key), value_cb, folly::none);
  }
}

void logdevice_vcs_get_latest_config(logdevice_vcs_t* vcs, const char* key,
                                     HsStablePtr mvar, HsInt cap,
                                     vcs_value_callback_data_t* cb_data) {
  auto value_cb = [mvar, cap, cb_data](facebook::logdevice::Status st,
                                       std::string val) {
    if (cb_data) {
      cb_data->st = static_cast<c_error_code_t>(st);
      // If status is OK, cb will be invoked with the value.
      // Otherwise, the value parameter is meaningless (but
      // default-constructed).
      if (st == facebook::logdevice::Status::OK) {
        cb_data->val_len = val.size();
        cb_data->value = copyString(val);
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };

  vcs->rep->getLatestConfig(std::string(key), value_cb);
}

/*
 * VersionedConfigStore provides strict conditional update semantics--it
 * will only update the value for a key if the base_version matches the latest
 * version in the store.
 *
 * @param key: key of the config
 * @param value:
 *   value to be stored. Note that the callsite need not guarantee the
 *   validity of the underlying buffer till callback is invoked.
 * @param base_version: Read the documentation Condition.
 * @param cb:
 *   callback void(Status, version_t, std::string value) that will be invoked
 *   if the status is one of:
 *     OK
 *     NOTFOUND // only possible when base_version.hasValue()
 *     VERSION_MISMATCH
 *     ACCESS
 *     AGAIN
 *     BADMSG // see implementation notes below
 *     INVALID_PARAM // see implementation notes below
 *     INVALID_CONFIG // see implementation notes below
 *     SHUTDOWN
 *   If status is OK, cb will be invoked with the version of the newly written
 *   config. If status is VERSION_MISMATCH, cb will be invoked with the
 *   version that caused the mismatch as well as the existing config, if
 *   available (i.e., always check in the callback whether version is
 *   EMPTY_VERSION). Otherwise, the version and value parameter(s) are
 *   meaningless (default-constructed).
 *
 */
void logdevice_vcs_update_config(
    logdevice_vcs_t* vcs, const char* key,
    // value
    const char* value, HsInt offset, HsInt val_len,
    // VersionedConfigStore::Condition
    //
    // - condition_mode 1 : VERSION
    // - condition_mode 2 : OVERWRITE
    // - condition_mode 3 : IF_NOT_EXISTS
    //
    // If condition_mode is 2 or 3, then the version will be ignored.
    HsInt condition_mode, c_vcs_config_version_t version,
    // VersionedConfigStore::Condition END
    HsStablePtr mvar, HsInt cap, vcs_write_callback_data_t* cb_data) {
  auto cb = [mvar, cap, cb_data](facebook::logdevice::Status st,
                                 vcs_config_version_t version,
                                 std::string val) {
    if (cb_data) {
      cb_data->st = static_cast<c_error_code_t>(st);
      if (st == Status::OK || st == Status::VERSION_MISMATCH) {
        cb_data->version = version.val_;
        cb_data->val_len = val.size();
        cb_data->value = copyString(val);
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  if (condition_mode == 2) {
    vcs->rep->updateConfig(std::string(key),
                           std::string(value + offset, val_len),
                           VersionedConfigStore::Condition::overwrite(), cb);
  } else if (condition_mode == 3) {
    vcs->rep->updateConfig(
        std::string(key), std::string(value + offset, val_len),
        VersionedConfigStore::Condition::createIfNotExists(), cb);
  } else {
    vcs->rep->updateConfig(
        std::string(key), std::string(value + offset, val_len),
        VersionedConfigStore::Condition(vcs_config_version_t(version)), cb);
  }
}

// ----------------------------------------------------------------------------
} // end extern "C"
