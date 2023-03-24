#include "hs_logdevice.h"

extern "C" {
// ----------------------------------------------------------------------------
// Reader & SyncCheckpointedReader

#define HSTREAM_USE_SHARED_CHECKPOINT_STORE

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
#ifdef HSTREAM_USE_SHARED_CHECKPOINT_STORE
  SharedCheckpointedReaderBase::CheckpointingOptions opts;
#else
  CheckpointedReaderBase::CheckpointingOptions opts;
#endif
  opts.num_retries = num_retries;
  const std::string reader_name_ = std::string(reader_name);
#ifdef HSTREAM_USE_SHARED_CHECKPOINT_STORE
  auto scr = CheckpointedReaderFactory().createCheckpointedReader(
      reader_name_, std::move(reader->rep), store->rep, opts);
#else
  auto scr = CheckpointedReaderFactory().createCheckpointedReader(
      reader_name_, std::move(reader->rep), std::move(store->rep), opts);
#endif
  logdevice_sync_checkpointed_reader_t* result =
      new logdevice_sync_checkpointed_reader_t;
  result->rep = std::move(scr);
  return result;
}

void free_sync_checkpointed_reader(logdevice_sync_checkpointed_reader_t* p) {
  delete p;
}

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

/**
 * If called, data records read by this Reader will not include payloads.
 *
 * This makes reading more efficient when payloads are not needed (they won't
 * be transmitted over the network).
 *
 * Only affects subsequent startReading() calls.
 */
#define WITHOUT_PAYLOAD(FuncName, ClassName)                                   \
  void FuncName(ClassName* reader) { return reader->rep->withoutPayload(); }
WITHOUT_PAYLOAD(ld_reader_without_payload, logdevice_reader_t)
WITHOUT_PAYLOAD(ld_ckp_reader_without_payload,
                logdevice_sync_checkpointed_reader_t)

/**
 * If called, data records read by this Reader will start including
 * approximate amount of data written to given log up to current record
 * once it become available to Reader.
 *
 * The value itself stored in DataRecord::attrs::byte_offset. Set as
 * BYTE_OFFSET_INVALID if unavailable to Reader yet.
 *
 * Only affects subsequent startReading() calls.
 */
#define INCLUDE_BYTEOFFSET(FuncName, ClassName)                                \
  void FuncName(ClassName* reader) { return reader->rep->includeByteOffset(); }
INCLUDE_BYTEOFFSET(ld_reader_include_byteoffset, logdevice_reader_t)
INCLUDE_BYTEOFFSET(ld_ckp_reader_include_byteoffset,
                   logdevice_sync_checkpointed_reader_t)

/**
 * If called, whenever read() can return some records but not the number
 * requested by the caller, it will return the records instead of waiting
 * for more.
 *
 * Example:
 * - Caller calls read(100, ...) asking for 100 data records.
 * - Only 20 records are immediately available.
 * - By default, read() would wait until 80 more records arrive or the
 *   timeout expires.  This makes sense for callers that can benefit from
 *   reading and processing batches of data records.
 * - If this method was called before read(), it would return the 20 records
 *   without waiting for more.  This may make sense for cases where latency
 *   is more important.
 */
#define WAIT_ONLY_WHEN_NO_DATA(FuncName, ClassName)                            \
  void FuncName(ClassName* reader) { return reader->rep->waitOnlyWhenNoData(); }
WAIT_ONLY_WHEN_NO_DATA(ld_reader_wait_only_when_no_data, logdevice_reader_t)
WAIT_ONLY_WHEN_NO_DATA(ld_ckp_reader_wait_only_when_no_data,
                       logdevice_sync_checkpointed_reader_t)

#define READ(FuncName, ClassName)                                              \
  facebook::logdevice::Status FuncName(                                        \
      ClassName* reader, size_t maxlen, logdevice_data_record_t* data_out,     \
      logdevice_gap_record_t* gap_out, ssize_t* len_out) {                     \
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
        data_out[i].timestamp = attrs.timestamp.count();                       \
        data_out[i].batch_offset = attrs.batch_offset;                         \
        data_out[i].payload = copyString(payload.toString());                  \
        data_out[i].payload_len = payload.size();                              \
        data_out[i].byte_offset =                                              \
            attrs.offsets.getCounter(facebook::logdevice::BYTE_OFFSET);        \
        ++i;                                                                   \
      }                                                                        \
    } /* A gap in the numbering sequence. */                                   \
    else {                                                                     \
      assert(facebook::logdevice::err == facebook::logdevice::E::GAP);         \
      if (gap_out) {                                                           \
        gap_out->logid = gap.logid.val_;                                       \
        gap_out->gaptype = static_cast<uint8_t>(gap.type);                     \
        gap_out->lo = gap.lo;                                                  \
        gap_out->hi = gap.hi;                                                  \
      } else {                                                                 \
        /* Warn about data loss but ignore other types of gaps. */             \
        if (gap.type == facebook::logdevice::GapType::DATALOSS) {              \
          fprintf(stderr, "warning: DATALOSS gaps for LSN range [%ld, %ld]\n", \
                  gap.lo, gap.hi);                                             \
          return facebook::logdevice::E::DATALOSS;                             \
        }                                                                      \
      }                                                                        \
    }                                                                          \
                                                                               \
    return facebook::logdevice::E::OK;                                         \
  }
READ(logdevice_reader_read, logdevice_reader_t)
READ(logdevice_checkpointed_reader_read, logdevice_sync_checkpointed_reader_t)

// ----------------------------------------------------------------------------
// SyncCheckpointedReader

facebook::logdevice::Status ld_checkpointed_reader_start_reading_from_ckp(
    logdevice_sync_checkpointed_reader_t* reader, c_logid_t logid,
    c_lsn_t until) {
  int ret =
      reader->rep->startReadingFromCheckpoint(logid_t(logid), until, nullptr);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_start_reading_from_ckp_or_start(logdevice_sync_checkpointed_reader_t* reader,
                                   c_logid_t logid, c_lsn_t start,
                                   c_lsn_t until) {
  int ret = reader->rep->startReadingFromCheckpoint(logid_t(logid), start,
                                                    until, nullptr);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

// ----------------------------------------------------------------------------
// CheckpointedReaderBase

void crb_write_checkpoints(logdevice_sync_checkpointed_reader_t* reader,
                           c_logid_t* logids, c_lsn_t* lsns, size_t len,
                           HsStablePtr mvar, HsInt cap,
                           facebook::logdevice::Status* st_out) {
  std::map<logid_t, lsn_t> checkpoints;
  for (int i = 0; i < len; ++i) {
    checkpoints[logid_t(logids[i])] = lsns[i];
  }
  auto cb = [st_out, mvar, cap](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  reader->rep->asyncWriteCheckpoints(checkpoints, cb);
}

void crb_write_last_read_checkpoints(
    logdevice_sync_checkpointed_reader_t* reader, const c_logid_t* logids,
    size_t len, HsStablePtr mvar, HsInt cap,
    facebook::logdevice::Status* st_out) {
  auto cb = [st_out, mvar, cap](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  reader->rep->asyncWriteCheckpoints(
      cb, std::vector<logid_t>(logids, logids + len));
}

void crb_asyncRemoveCheckpoints(logdevice_sync_checkpointed_reader_t* reader,
                                const c_logid_t* logids, size_t len,
                                HsStablePtr mvar, HsInt cap,
                                facebook::logdevice::Status* st_out) {
  auto cb = [st_out, mvar, cap](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  reader->rep->asyncRemoveCheckpoints(
      std::vector<logid_t>(logids, logids + len), cb);
}

void crb_asyncRemoveAllCheckpoints(logdevice_sync_checkpointed_reader_t* reader,
                                   HsStablePtr mvar, HsInt cap,
                                   facebook::logdevice::Status* st_out) {
  auto cb = [st_out, mvar, cap](facebook::logdevice::Status st) {
    if (st_out) {
      *st_out = st;
    }
    hs_try_putmvar(cap, mvar);
  };
  reader->rep->asyncRemoveAllCheckpoints(cb);
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

// ----------------------------------------------------------------------------
} // end extern "C"
