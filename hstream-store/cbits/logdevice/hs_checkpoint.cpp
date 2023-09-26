#include "hs_logdevice.h"

extern "C" {

// ----------------------------------------------------------------------------
// Checkpoint Store

// Note: it's not safe to have multiple FileBasedVersionedConfigStore
// objects created from the `root_path' accessing configs with the same
// `key' concurrently. For the best practice, use only one
// FileBasedVersionedConfigStore instance for one `root_path'.

logdevice_checkpoint_store_t*
new_file_based_checkpoint_store(const char* root_path) {
  std::string root_path_ = std::string(root_path);
#ifdef HSTREAM_USE_SHARED_CHECKPOINT_STORE
  std::shared_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createSharedFileBasedCheckpointStore(root_path_);
#else
  std::unique_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createFileBasedCheckpointStore(root_path_);
#endif
  logdevice_checkpoint_store_t* result = new logdevice_checkpoint_store_t;
  result->rep = std::move(checkpoint_store);
  return result;
}

logdevice_checkpoint_store_t*
new_rsm_based_checkpoint_store(logdevice_client_t* client, c_logid_t log_id,
                               int64_t stop_timeout) {
  std::chrono::milliseconds ms(stop_timeout);
#ifdef HSTREAM_USE_SHARED_CHECKPOINT_STORE
  std::shared_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createSharedRSMBasedCheckpointStore(
          client->rep, logid_t(log_id), ms);
#else
  std::unique_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createRSMBasedCheckpointStore(
          client->rep, logid_t(log_id), ms);
#endif
  logdevice_checkpoint_store_t* result = new logdevice_checkpoint_store_t;
  result->rep = std::move(checkpoint_store);
  return result;
}

logdevice_checkpoint_store_t*
new_zookeeper_based_checkpoint_store(logdevice_client_t* client) {
  throw std::logic_error("There is no zk based checkpoint store.");
  // std::unique_ptr<CheckpointStore> checkpoint_store =
  //     CheckpointStoreFactory().createZookeeperBasedCheckpointStore(client->rep);
  // logdevice_checkpoint_store_t* result = new logdevice_checkpoint_store_t;
  // result->rep = std::move(checkpoint_store);
  // return result;
}

void free_checkpoint_store(logdevice_checkpoint_store_t* p) { delete p; }

void checkpoint_store_get_lsn(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              HsStablePtr mvar, HsInt cap,
                              facebook::logdevice::Status* st_out,
                              c_lsn_t* value_out) {
  std::string customer_id_ = std::string(customer_id);
  auto cb = [st_out, value_out, mvar, cap](facebook::logdevice::Status st,
                                           lsn_t lsn) {
    if (st_out && value_out) {
      *st_out = st;
      *value_out = lsn;
    }
    hs_try_putmvar(cap, mvar);
  };
  store->rep->getLSN(customer_id_, logid_t(logid), cb);
}

void checkpoint_store_get_all_checkpoints(
    logdevice_checkpoint_store_t* store, const char* customer_id,
    HsStablePtr mvar, HsInt cap, facebook::logdevice::Status* st_out,
    HsInt* len, c_logid_t** keys_ptr, c_lsn_t** values_ptr,
    std::vector<c_logid_t>** keys_, std::vector<c_lsn_t>** values_) {
  auto cb = [st_out, cap, mvar, len, keys_ptr, values_ptr, keys_,
             values_](facebook::logdevice::Status st,
                      std::map<c_logid_t, c_lsn_t>& log_lsn_map) {
    if (st_out) {
      *st_out = st;
    }
    if (st == ld::Status::OK) {
      std::vector<c_logid_t>* keys = new std::vector<c_logid_t>;
      std::vector<c_lsn_t>* values = new std::vector<c_lsn_t>;

      for (const auto& [key, value] : log_lsn_map) {
        keys->push_back(key);
        values->push_back(value);
      }

      *len = keys->size();
      *keys_ptr = keys->data();
      *values_ptr = values->data();
      *keys_ = keys;
      *values_ = values;
    }
    hs_try_putmvar(cap, mvar);
  };
  store->rep->getAllCheckpoints(std::string(customer_id), std::move(cb));
}

void checkpoint_store_update_lsn(logdevice_checkpoint_store_t* store,
                                 const char* customer_id, c_logid_t logid,
                                 c_lsn_t lsn, HsStablePtr mvar, HsInt cap,
                                 facebook::logdevice::Status* st_out) {
  std::string customer_id_ = std::string(customer_id);
  auto cb = [st_out, cap, mvar](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  store->rep->updateLSN(customer_id_, logid_t(logid), lsn, cb);
}

void checkpoint_store_update_multi_lsn(logdevice_checkpoint_store_t* store,
                                       const char* customer_id,
                                       c_logid_t* logids, c_lsn_t* lsns,
                                       size_t len, HsStablePtr mvar, HsInt cap,
                                       facebook::logdevice::Status* st_out) {
  std::string customer_id_ = std::string(customer_id);
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i)
    checkpoints[logid_t(logids[i])] = lsns[i];
  auto cb = [st_out, cap, mvar](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };

  return store->rep->updateLSN(customer_id_, checkpoints, cb);
}

void checkpoint_store_remove_checkpoints(logdevice_checkpoint_store_t* store,
                                         const char* customer_id,
                                         c_logid_t* logids, HsInt logid_offset,
                                         HsInt logid_len, HsStablePtr mvar,
                                         HsInt cap,
                                         facebook::logdevice::Status* st_out) {
  std::string customer_id_ = std::string(customer_id);
  logids += logid_offset;
  std::vector<logid_t> checkpoints;
  checkpoints.reserve(logid_len);
  for (int i = 0; i < logid_len; i++) {
    checkpoints.push_back(logid_t(*logids));
  }
  auto cb = [st_out, cap, mvar](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  store->rep->removeCheckpoints(customer_id_, checkpoints, cb);
}

void checkpoint_store_remove_all_checkpoints(
    logdevice_checkpoint_store_t* store, const char* customer_id,
    HsStablePtr mvar, HsInt cap, facebook::logdevice::Status* st_out) {
  std::string customer_id_ = std::string(customer_id);
  auto cb = [st_out, cap, mvar](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  store->rep->removeAllCheckpoints(customer_id_, cb);
}

// ----------------------------------------------------------------------------

facebook::logdevice::Status
checkpoint_store_get_lsn_sync(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              c_lsn_t* value_out) {
  std::string customer_id_ = std::string(customer_id);
  facebook::logdevice::Status ret =
      store->rep->getLSNSync(customer_id_, logid_t(logid), value_out);
  return ret;
}

facebook::logdevice::Status
checkpoint_store_update_lsn_sync(logdevice_checkpoint_store_t* store,
                                 const char* customer_id, c_logid_t logid,
                                 c_lsn_t lsn) {
  std::string customer_id_ = std::string(customer_id);
  facebook::logdevice::Status ret =
      store->rep->updateLSNSync(customer_id_, logid_t(logid), lsn);
  return ret;
}

facebook::logdevice::Status checkpoint_store_update_multi_lsn_sync(
    logdevice_checkpoint_store_t* store, const char* customer_id,
    c_logid_t* logids, c_lsn_t* lsns, size_t len) {
  std::string customer_id_ = std::string(customer_id);
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i)
    checkpoints[logid_t(logids[i])] = lsns[i];
  return store->rep->updateLSNSync(customer_id_, checkpoints);
}

// ----------------------------------------------------------------------------
} // end extern "C"
