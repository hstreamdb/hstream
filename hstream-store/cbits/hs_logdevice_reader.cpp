#include "hs_logdevice.h"

char* copyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

extern "C" {
// ----------------------------------------------------------------------------

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

facebook::logdevice::Status ld_reader_start_reading(logdevice_reader_t* reader,
                                                    c_logid_t logid,
                                                    c_lsn_t start,
                                                    c_lsn_t until) {
  int ret = reader->rep->startReading(facebook::logdevice::logid_t(logid),
                                      start, until);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status ld_reader_stop_reading(logdevice_reader_t* reader,
                                                   c_logid_t logid) {
  int ret = reader->rep->stopReading(facebook::logdevice::logid_t(logid));
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

bool ld_reader_is_reading(logdevice_reader_t* reader, c_logid_t logid) {
  return reader->rep->isReading(facebook::logdevice::logid_t(logid));
}

bool ld_reader_is_reading_any(logdevice_reader_t* reader) {
  return reader->rep->isReadingAny();
}

// NOTE that the maximum timeout is 2^31-1 milliseconds.
int ld_reader_set_timeout(logdevice_reader_t* reader, int32_t timeout) {
  std::chrono::milliseconds t = std::chrono::milliseconds(timeout);
  return reader->rep->setTimeout(t);
}

facebook::logdevice::Status
logdevice_reader_read(logdevice_reader_t* reader, size_t maxlen,
                      logdevice_data_record_t* data_out, ssize_t* len_out) {
  std::vector<std::unique_ptr<DataRecord>> data;
  facebook::logdevice::GapRecord gap;

  ssize_t nread = reader->rep->read(maxlen, &data, &gap);
  *len_out = nread;
  // Copy data record
  if (nread >= 0) {
    size_t i = 0;
    for (auto& record_ptr : data) {
      const facebook::logdevice::Payload& payload = record_ptr->payload;
      const facebook::logdevice::DataRecordAttributes& attrs =
          record_ptr->attrs;
      facebook::logdevice::logid_t& logid = record_ptr->logid;
      data_out[i].logid = logid.val_;
      data_out[i].lsn = attrs.lsn;
      data_out[i].payload = copyString(payload.toString());
      data_out[i].payload_len = payload.size();
      ++i;
    }
  }
  // A gap in the numbering sequence. Warn about data loss but ignore
  // other types of gaps.
  else {
    assert(facebook::logdevice::err == facebook::logdevice::E::GAP);
    if (gap.type == facebook::logdevice::GapType::DATALOSS) {
      fprintf(stderr, "warning: DATALOSS gaps for LSN range [%ld, %ld]\n",
              gap.lo, gap.hi);
      return facebook::logdevice::E::DATALOSS;
    }
  }

  return facebook::logdevice::E::OK;
}

// ----------------------------------------------------------------------------
} // end extern "C"
