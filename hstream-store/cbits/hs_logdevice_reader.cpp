#include "hs_logdevice.h"

char* copyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

extern "C" {
// ----------------------------------------------------------------------------
// Reader

logdevice_reader_t* new_logdevice_reader(logdevice_client_t* client,
                                         size_t max_logs, ssize_t buffer_size) {
  std::unique_ptr<Reader> reader;
  reader = client->rep->createReader(max_logs, buffer_size);
  if (!reader) {
    std::cerr << "-> new_logdevice_reader error!\n";
    exit(1);
  }
  logdevice_reader_t* result = new logdevice_reader_t;
  result->rep = std::move(reader);
  return result;
}

void free_logdevice_reader(logdevice_reader_t* reader) { delete reader; }

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

// ----------------------------------------------------------------------------
// Reader & CheckpointedReader

#define START_READING(FuncName, ClassName)                                     \
  facebook::logdevice::Status FuncName(ClassName* reader, c_logid_t logid,     \
                                       c_lsn_t start, c_lsn_t until) {         \
    int ret = reader->rep->startReading(logid_t(logid), start, until);         \
    if (ret == 0)                                                              \
      return facebook::logdevice::E::OK;                                       \
    return facebook::logdevice::err;                                           \
  }
START_READING(ld_reader_start_reading, logdevice_reader_t)
START_READING(ld_checkpointed_reader_start_reading,
              logdevice_sync_checkpointed_reader_t)

#define STOP_READING(FuncName, ClassName)                                      \
  facebook::logdevice::Status FuncName(ClassName* reader, c_logid_t logid) {   \
    int ret = reader->rep->stopReading(logid_t(logid));                        \
    if (ret == 0)                                                              \
      return facebook::logdevice::E::OK;                                       \
    return facebook::logdevice::err;                                           \
  }
STOP_READING(ld_reader_stop_reading, logdevice_reader_t)
STOP_READING(ld_checkpointed_reader_stop_reading,
             logdevice_sync_checkpointed_reader_t)

#define IS_READING(FuncName, ClassName)                                        \
  bool FuncName(ClassName* reader, c_logid_t logid) {                          \
    return reader->rep->isReading(logid_t(logid));                             \
  }
IS_READING(ld_reader_is_reading, logdevice_reader_t)
IS_READING(ld_checkpointed_reader_is_reading,
           logdevice_sync_checkpointed_reader_t)

#define IS_READING_ANY(FuncName, ClassName)                                    \
  bool FuncName(ClassName* reader) { return reader->rep->isReadingAny(); }
IS_READING_ANY(ld_reader_is_reading_any, logdevice_reader_t)
IS_READING_ANY(ld_checkpointed_reader_is_reading_any,
               logdevice_sync_checkpointed_reader_t)

// NOTE that the maximum timeout is 2^31-1 milliseconds.
#define SET_TIMEOUT(FuncName, ClassName)                                       \
  int FuncName(ClassName* reader, int32_t timeout) {                           \
    std::chrono::milliseconds t = std::chrono::milliseconds(timeout);          \
    return reader->rep->setTimeout(t);                                         \
  }
SET_TIMEOUT(ld_reader_set_timeout, logdevice_reader_t)
SET_TIMEOUT(ld_checkpointed_reader_set_timeout,
            logdevice_sync_checkpointed_reader_t)

#define READ(FuncName, ClassName)                                              \
  facebook::logdevice::Status FuncName(ClassName* reader, size_t maxlen,       \
                                       logdevice_data_record_t* data_out,      \
                                       ssize_t* len_out) {                     \
    std::vector<std::unique_ptr<DataRecord>> data;                             \
    facebook::logdevice::GapRecord gap;                                        \
                                                                               \
    ssize_t nread = reader->rep->read(maxlen, &data, &gap);                    \
    *len_out = nread;                                                          \
    /* Copy data record  */                                                    \
    if (nread >= 0) {                                                          \
      size_t i = 0;                                                            \
      for (auto& record_ptr : data) {                                          \
        const facebook::logdevice::Payload& payload = record_ptr->payload;     \
        const facebook::logdevice::DataRecordAttributes& attrs =               \
            record_ptr->attrs;                                                 \
        logid_t& logid = record_ptr->logid;                                    \
        data_out[i].logid = logid.val_;                                        \
        data_out[i].lsn = attrs.lsn;                                           \
        data_out[i].payload = copyString(payload.toString());                  \
        data_out[i].payload_len = payload.size();                              \
        ++i;                                                                   \
      }                                                                        \
    } /* A gap in the numbering sequence. Warn about data loss but ignore      \
         other types of gaps. */                                               \
    else {                                                                     \
      assert(facebook::logdevice::err == facebook::logdevice::E::GAP);         \
      if (gap.type == facebook::logdevice::GapType::DATALOSS) {                \
        fprintf(stderr, "warning: DATALOSS gaps for LSN range [%ld, %ld]\n",   \
                gap.lo, gap.hi);                                               \
        return facebook::logdevice::E::DATALOSS;                               \
      }                                                                        \
    }                                                                          \
                                                                               \
    return facebook::logdevice::E::OK;                                         \
  }
READ(logdevice_reader_read, logdevice_reader_t)
READ(logdevice_checkpointed_reader_read, logdevice_sync_checkpointed_reader_t)

// ----------------------------------------------------------------------------
// Checkpointed Reader

facebook::logdevice::Status
sync_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                       c_logid_t* logids, c_lsn_t* lsns, size_t len) {
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i)
    checkpoints[logid_t(logids[i])] = lsns[i];
  return reader->rep->syncWriteCheckpoints(checkpoints);
}

void write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                       c_logid_t* logids, c_lsn_t* lsns, size_t len,
                       HsStablePtr mvar, HsInt cap,
                       facebook::logdevice::Status* st_out) {
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i)
    checkpoints[logid_t(logids[i])] = lsns[i];
  auto cb = [&](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  reader->rep->asyncWriteCheckpoints(checkpoints, cb);
}

facebook::logdevice::Status
sync_write_last_read_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                                 const c_logid_t* logids, size_t len) {
  return reader->rep->syncWriteCheckpoints(
      std::vector<logid_t>(logids, logids + len));
}

// ----------------------------------------------------------------------------
} // end extern "C"
