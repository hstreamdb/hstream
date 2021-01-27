#include "hs_logdevice.h"

// ----------------------------------------------------------------------------
// Checkpoint Store

// Note: it's not safe to have multiple FileBasedVersionedConfigStore
// objects created from the `root_path' accessing configs with the same
// `key' concurrently. For the best practice, use only one
// FileBasedVersionedConfigStore instance for one `root_path'.

logdevice_checkpoint_store_t*
new_file_based_checkpoint_store(const char* root_path) {
  std::string root_path_ = std::string(root_path);
  std::unique_ptr<CheckpointStore> checkpoint_store =
      CheckpointStoreFactory().createFileBasedCheckpointStore(root_path_);
  logdevice_checkpoint_store_t* result = new logdevice_checkpoint_store_t;
  result->rep = std::move(checkpoint_store);
  return result;
}

void free_checkpoint_store(logdevice_checkpoint_store_t* p) { delete p; }

facebook::logdevice::Status
checkpoint_store_get_lsn_sync(logdevice_checkpoint_store_t* store,
                              const char* customer_id, c_logid_t logid,
                              c_lsn_t* value_out) {
  std::string customer_id_ = std::string(customer_id);
  facebook::logdevice::Status ret = store->rep->getLSNSync(customer_id_, logid_t(logid), value_out);
  return ret;
}

facebook::logdevice::Status
checkpoint_store_update_lsn_sync(logdevice_checkpoint_store_t* store,
                                 const char* customer_id, c_logid_t logid,
                                 c_lsn_t lsn) {
  std::string customer_id_ = std::string(customer_id);
  facebook::logdevice::Status ret = store->rep->updateLSNSync(customer_id_, logid_t(logid), lsn);
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
// Checkpointed Reader

logdevice_sync_checkpointed_reader_t* new_sync_checkpointed_reader(
    const char* reader_name, logdevice_reader_t* reader,
    logdevice_checkpoint_store_t* store, uint32_t num_retries) {
  CheckpointedReaderBase::CheckpointingOptions opts;
  opts.num_retries = num_retries;
  const std::string reader_name_ = std::string(reader_name);
  std::unique_ptr<SyncCheckpointedReader> scr =
      CheckpointedReaderFactory().createCheckpointedReader(
          reader_name_, std::move(reader->rep), std::move(store->rep), opts);
  logdevice_sync_checkpointed_reader_t* result =
      new logdevice_sync_checkpointed_reader_t;
  result->rep = std::move(scr);
  return result;
}

void free_sync_checkpointed_reader(logdevice_sync_checkpointed_reader_t* p) {
  delete p;
}

facebook::logdevice::Status
sync_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                       c_logid_t* logids, c_lsn_t* lsns, size_t len) {
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i)
    checkpoints[logid_t(logids[i])] = lsns[i];
  return reader->rep->syncWriteCheckpoints(checkpoints);
}

facebook::logdevice::Status
sync_write_last_read_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                                 const c_logid_t* logids, size_t len) {
  return reader->rep->syncWriteCheckpoints(
      std::vector<logid_t>(logids, logids + len));
}
