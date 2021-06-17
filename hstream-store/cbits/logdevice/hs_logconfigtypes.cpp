#include "hs_logdevice.h"

extern "C" {

facebook::logdevice::Status
ld_client_set_attributes(logdevice_client_t* client, const char* path,
                         LogAttributes* attrs, HsStablePtr mvar, HsInt cap,
                         logsconfig_status_cb_data_t* data);

// ----------------------------------------------------------------------------
// LogAttributes

#if __GLASGOW_HASKELL__ < 810
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgMutArrPtrs* keys_, StgMutArrPtrs* vals_) {
  StgArrBytes** keys = (StgArrBytes**)keys_->payload;
  StgArrBytes** vals = (StgArrBytes**)vals_->payload;
#else
LogAttributes* new_log_attributes(int replicationFactor, HsInt extras_len,
                                  StgArrBytes** keys, StgArrBytes** vals) {
#endif
  auto attrs = LogAttributes().with_replicationFactor(replicationFactor);
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

void free_log_attributes(LogAttributes* attrs) { delete attrs; }

bool exist_log_attrs_extras(LogAttributes* attrs, char* key) {
  if (attrs) {
    auto extras = attrs->extras();
    return extras.hasValue() && extras.value().contains(key);
  }
  return false;
}

// get extras without check the key exists.
std::string* get_log_attrs_extra(LogAttributes* attrs, char* key) {
  auto extras = attrs->extras().value();
  return new_hs_std_string(std::move(extras[key]));
}

#if __GLASGOW_HASKELL__ < 810
LogAttributes* update_log_attrs_extras(LogAttributes* attrs, HsInt extras_len,
                                       StgMutArrPtrs* keys_,
                                       StgMutArrPtrs* vals_) {
  StgArrBytes** keys = (StgArrBytes**)keys_->payload;
  StgArrBytes** vals = (StgArrBytes**)vals_->payload;
#else
LogAttributes* update_log_attrs_extras(LogAttributes* attrs, HsInt extras_len,
                                       StgArrBytes** keys, StgArrBytes** vals) {
#endif
  LogAttributes::ExtrasMap new_extras = attrs->extras().value();
  if (extras_len > 0) {
    for (int i = 0; i < extras_len; ++i) {
      new_extras[(char*)(keys[i]->payload)] = (char*)(vals[i]->payload);
    }
  }
  LogAttributes* attrs_ = new LogAttributes(attrs->with_extras(new_extras));
  return attrs_;
}

// TODO: macro
int get_replication_factor(LogAttributes* attrs) {
  return attrs->replicationFactor().value();
}

std::string* describe_log_maxWritesInFlight(LogAttributes* attrs) {
  return new_hs_std_string(attrs->maxWritesInFlight().describe());
}

void get_attribute_extras(LogAttributes* attrs, size_t* len,
                          std::string** keys_ptr, std::string** values_ptr,
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
// LogHeadAttributes

facebook::logdevice::Status
get_head_attributes(logdevice_client_t* client, c_logid_t logid,
                    HsStablePtr mvar, HsInt cap,
                    log_head_attributes_cb_data_t* data) {
  auto cb = [data, cap,
             mvar](facebook::logdevice::Status st,
                   std::unique_ptr<LogHeadAttributes> head_attrs_ptr) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      if (head_attrs_ptr) {
        data->head_attributes = new logdevice_log_head_attributes_t;
        data->head_attributes->rep = std::move(head_attrs_ptr);
      } else {
        data->head_attributes = NULL;
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret =
      client->rep->getHeadAttributes(facebook::logdevice::logid_t(logid), cb);
  if (ret == 0) {
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
}

void free_log_head_attributes(logdevice_log_head_attributes_t* p) { delete p; }

c_timestamp_t
get_trim_point_timestamp(logdevice_log_head_attributes_t* head_attributes) {
  return head_attributes->rep->trim_point_timestamp.count();
}

c_lsn_t get_trim_point(logdevice_log_head_attributes_t* head_attributes) {
  return head_attributes->rep->trim_point;
}

// ----------------------------------------------------------------------------
// LogTailAttributes

bool valid_log_tail_attributes(logdevice_log_tail_attributes_t* attributes) {
  return attributes->last_released_real_lsn !=
             facebook::logdevice::LSN_INVALID &&
         attributes->last_timestamp != c_timestamp_t{0};
}

facebook::logdevice::Status
ld_client_get_tail_attributes(logdevice_client_t* client, c_logid_t logid,
                              HsStablePtr mvar, HsInt cap,
                              log_tail_attributes_cb_data_t* cb_data) {
  auto cb = [cb_data, cap,
             mvar](facebook::logdevice::Status st,
                   std::unique_ptr<LogTailAttributes> tail_attr_ptr) {
    if (cb_data) {
      cb_data->st = static_cast<c_error_code_t>(st);
      if (tail_attr_ptr) {
        auto tail_attrs = new logdevice_log_tail_attributes_t;
        tail_attrs->last_released_real_lsn =
            tail_attr_ptr->last_released_real_lsn;
        tail_attrs->last_timestamp = tail_attr_ptr->last_timestamp.count();
        tail_attrs->offsets =
            tail_attr_ptr->offsets.getCounter(facebook::logdevice::BYTE_OFFSET);
        cb_data->tail_attributes = tail_attrs;
      } else {
        cb_data->tail_attributes = NULL;
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };

  int ret = client->rep->getTailAttributes(logid_t(logid), cb);
  if (ret == 0) {
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
}

lsn_t ld_client_get_tail_attributes_lsn(
    logdevice_log_tail_attributes_t* tail_attr) {
  return tail_attr->last_released_real_lsn;
}

c_timestamp_t ld_client_get_tail_attributes_last_timestamp(
    logdevice_log_tail_attributes_t* tail_attr) {
  return tail_attr->last_timestamp;
}

uint64_t ld_client_get_tail_attributes_bytes_offset(
    logdevice_log_tail_attributes_t* tail_attr) {
  return tail_attr->offsets;
}

void free_logdevice_tail_attributes(
    logdevice_log_tail_attributes_t* tail_attr) {
  delete tail_attr;
}

// ----------------------------------------------------------------------------
// LogConfigType: LogGroup

facebook::logdevice::Status ld_client_make_loggroup(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    HsStablePtr mvar, HsInt cap, make_loggroup_cb_data_t* data) {
  std::string path_ = path;
  auto start = facebook::logdevice::logid_t(start_logid);
  auto end = facebook::logdevice::logid_t(end_logid);
  auto cb = [data, cap, mvar](facebook::logdevice::Status st,
                              std::unique_ptr<LogGroup> loggroup_ptr,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      if (loggroup_ptr) {
        data->loggroup = new logdevice_loggroup_t;
        data->loggroup->rep = std::move(loggroup_ptr);
      } else {
        data->loggroup = NULL;
      }
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->makeLogGroup(path_, std::make_pair(start, end), *attrs,
                                      mk_intermediate_dirs, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

void ld_client_get_loggroup(logdevice_client_t* client, const char* path,
                            HsStablePtr mvar, HsInt cap,
                            facebook::logdevice::Status* st_out,
                            logdevice_loggroup_t** loggroup_result) {
  std::string path_ = path;
  auto cb = [st_out, loggroup_result, cap,
             mvar](facebook::logdevice::Status st,
                   std::unique_ptr<LogGroup> loggroup_ptr) {
    *st_out = st;
    if (loggroup_result && loggroup_ptr) {
      *loggroup_result = new logdevice_loggroup_t;
      (*loggroup_result)->rep = std::move(loggroup_ptr);
    } else {
      *loggroup_result = NULL;
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  client->rep->getLogGroup(path_, cb);
}

void ld_client_get_loggroup_by_id(logdevice_client_t* client, c_logid_t logid,
                                  HsStablePtr mvar, HsInt cap,
                                  facebook::logdevice::Status* st_out,
                                  logdevice_loggroup_t** loggroup_result) {
  auto cb = [st_out, loggroup_result, mvar,
             cap](facebook::logdevice::Status st,
                  std::unique_ptr<LogGroup> loggroup) {
    *st_out = st;
    if (loggroup_result && loggroup) {
      *loggroup_result = new logdevice_loggroup_t;
      (*loggroup_result)->rep = std::move(loggroup);
    } else {
      *loggroup_result = NULL;
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  client->rep->getLogGroupById(logid_t(logid), cb);
}

facebook::logdevice::Status
ld_client_remove_loggroup(logdevice_client_t* client, const char* path,
                          HsStablePtr mvar, HsInt cap,
                          logsconfig_status_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [data, cap, mvar](facebook::logdevice::Status st, uint64_t version,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->version = version;
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->removeLogGroup(path, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

// LogGroup attrs : range
void ld_loggroup_get_range(logdevice_loggroup_t* group, c_logid_t* start,
                           c_logid_t* end) {
  const facebook::logdevice::logid_range_t& range = group->rep->range();
  *start = range.first.val();
  *end = range.second.val();
}

// FIXME: Do we need to copy the returned string?
const char* ld_loggroup_get_name(logdevice_loggroup_t* group) {
  return group->rep->name().c_str();
}

// FIXME: Do we need to copy the returned string?
const char* ld_loggroup_get_fully_qualified_name(logdevice_loggroup_t* group) {
  return group->rep->getFullyQualifiedName().c_str();
}

// FIXME: Do we need to copy the returned attrs?
const LogAttributes* ld_loggroup_get_attrs(logdevice_loggroup_t* group) {
  const LogAttributes& attrs = group->rep->attrs();
  return &attrs;
}

uint64_t ld_loggroup_get_version(logdevice_loggroup_t* group) {
  return group->rep->version();
}

#if __GLASGOW_HASKELL__ < 810
facebook::logdevice::Status ld_loggroup_update_extra_attrs(
    logdevice_client_t* client, logdevice_loggroup_t* group,
    //
    HsInt extras_len, StgMutArrPtrs* keys_, StgMutArrPtrs* vals_,
    //
    HsStablePtr mvar, HsInt cap, logsconfig_status_cb_data_t* data) {
  StgArrBytes** keys = (StgArrBytes**)keys_->payload;
  StgArrBytes** vals = (StgArrBytes**)vals_->payload;
#else
facebook::logdevice::Status ld_loggroup_update_extra_attrs(
    logdevice_client_t* client, logdevice_loggroup_t* group,
    //
    HsInt extras_len, StgArrBytes** keys, StgArrBytes** vals,
    //
    HsStablePtr mvar, HsInt cap, logsconfig_status_cb_data_t* data) {
#endif
  const std::string& path = group->rep->getFullyQualifiedName();
  const LogAttributes& logAttrs = group->rep->attrs();

  LogAttributes::ExtrasMap new_extras = logAttrs.extras().value();
  if (extras_len > 0) {
    for (int i = 0; i < extras_len; ++i) {
      new_extras[(char*)(keys[i]->payload)] = (char*)(vals[i]->payload);
    }
  }
  auto newLogAttrs = logAttrs.with_extras(new_extras);
  return ld_client_set_attributes(client, path.c_str(), &newLogAttrs, mvar, cap,
                                  data);
}

void free_logdevice_loggroup(logdevice_loggroup_t* group) { delete group; }

// ----------------------------------------------------------------------------
// LogConfigType: LogDirectory

facebook::logdevice::Status
ld_client_make_directory(logdevice_client_t* client, const char* path,
                         bool mk_intermediate_dirs, LogAttributes* attrs,
                         HsStablePtr mvar, HsInt cap,
                         make_directory_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [data, cap, mvar](facebook::logdevice::Status st,
                              std::unique_ptr<LogDirectory> directory_ptr,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      if (directory_ptr) {
        data->directory = new logdevice_logdirectory_t;
        data->directory->rep = std::move(directory_ptr);
      } else {
        data->directory = NULL;
      }
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->makeDirectory(path_, mk_intermediate_dirs, *attrs, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_remove_directory(logdevice_client_t* client, const char* path,
                           bool recursive, HsStablePtr mvar, HsInt cap,
                           logsconfig_status_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [data, cap, mvar](facebook::logdevice::Status st, uint64_t version,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->version = version;
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->removeDirectory(path_, recursive, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_get_directory(logdevice_client_t* client, const char* path,
                        HsStablePtr mvar, HsInt cap,
                        facebook::logdevice::Status* st_out,
                        logdevice_logdirectory_t** logdir_result) {
  std::string path_ = path;
  auto cb = [st_out, logdir_result, cap,
             mvar](facebook::logdevice::Status st,
                   std::unique_ptr<LogDirectory> logdir) {
    if (st_out && logdir_result) {
      *st_out = st;
      if (logdir) {
        logdevice_logdirectory_t* result = new logdevice_logdirectory_t;
        result->rep = std::move(logdir);
        *logdir_result = result;
      }
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->getDirectory(path_, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

void free_logdevice_logdirectory(logdevice_logdirectory_t* dir) { delete dir; }

#define DIR_MAP_ATTR_KEYS(FUN_NAME, ATTR_NAME)                                 \
  std::string* FUN_NAME(logdevice_logdirectory_t* dir, HsInt* ret_len,         \
                        const std::vector<std::string>** ret_raw_keys) {       \
    auto& valmap = dir->rep->ATTR_NAME();                                      \
    std::vector<std::string>* keys = getKeys(valmap);                          \
    *ret_len = keys->size();                                                   \
    *ret_raw_keys = keys;                                                      \
    return keys->data();                                                       \
  }
// LogDirectory attrs : children keys
DIR_MAP_ATTR_KEYS(ld_logdir_children_keys, children)
// LogDirectory attrs : logs keys
DIR_MAP_ATTR_KEYS(ld_logdir_logs_keys, logs)

// LogDirectory attrs : name
const char* ld_logdirectory_name(logdevice_logdirectory_t* dir) {
  return dir->rep->name().c_str();
}

// LogDirectory attrs : fullyQualifiedName
const char*
ld_logdirectory_full_name(logdevice_logdirectory_t* dir) {
  return dir->rep->getFullyQualifiedName().c_str();
}

// LogDirectory attrs : version
uint64_t ld_logdirectory_get_version(logdevice_logdirectory_t* dir) {
  return dir->rep->version();
}

const LogAttributes* ld_logdirectory_get_attrs(logdevice_logdirectory_t* dir) {
  const LogAttributes& attrs = dir->rep->attrs();
  return &attrs;
}

#define LD_LOGDIRECTORY_MAP_ATTR(NAME, ATTR_NAME, RET_TYPE, FIND_FUN, NOT_FUN) \
  RET_TYPE NAME(logdevice_logdirectory_t* dir, const char* key) {              \
    auto& attrMap = dir->rep->ATTR_NAME();                                     \
    auto value = attrMap.find(key);                                            \
    if (value != attrMap.end()) {                                              \
      return FIND_FUN;                                                         \
    } else {                                                                   \
      return NOT_FUN;                                                          \
    }                                                                          \
  }

// children name
// FIXME: Do we need to copy the returned string?
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_child_name, children, const char*,
                         value->second->name().c_str(), NULL)
// children fully qualified name
// FIXME: Do we need to copy the returned string?
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_child_full_name, children, const char*,
                         value->second->getFullyQualifiedName().c_str(), NULL)
// children verison
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_child_version, children, uint64_t,
                         (value->second->version()), 0)
// dir log name
// FIXME: Do we need to copy the returned string?
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_log_name, logs, const char*,
                         value->second->name().c_str(), NULL)
// dir log fully qualified name
// FIXME: Do we need to copy the returned string?
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_log_full_name, logs, const char*,
                         value->second->getFullyQualifiedName().c_str(), NULL)
// dir log verison
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_log_version, logs, uint64_t,
                         (value->second->version()), 0)
// dir log attrs
LD_LOGDIRECTORY_MAP_ATTR(ld_logdir_log_attrs, logs, const LogAttributes*,
                         (&(value->second->attrs())), NULL)
// dir log range
void ld_logdir_logs_range(logdevice_logdirectory_t* dir, const char* key,
                          c_logid_t* start, c_logid_t* end) {
  auto& attrMap = dir->rep->logs();
  auto value = attrMap.find(key);
  if (value != attrMap.end()) {
    auto& range = value->second->range();
    *start = range.first.val();
    *end = range.second.val();
  }
}

// helper function to get all loggroup names under a LogDirectory.
void get_logs_name(const std::unique_ptr<LogDirectory>& dir, bool rec,
                   std::vector<std::string>* lognames) {
  auto& logs = dir->logs();
  for (const auto& [key, value] : logs) {
    lognames->push_back(value->getFullyQualifiedName());
  }
  if (rec) {
    auto& dirs = dir->children();
    for (const auto& [key, value] : dirs) {
      get_logs_name(value, rec, lognames);
    }
  }
}

void ld_logdirectory_get_logs_name(logdevice_logdirectory_t* dir_,
                                   bool recursive,
                                   //
                                   size_t* len, std::string** names_ptr,
                                   //
                                   std::vector<std::string>** lognames_) {
  std::vector<std::string>* lognames = new std::vector<std::string>;
  get_logs_name(std::move(dir_->rep), recursive, lognames);

  *len = lognames->size();
  *names_ptr = lognames->data();
  *lognames_ = lognames;
}

// ----------------------------------------------------------------------------

facebook::logdevice::Status
ld_client_sync_logsconfig_version(logdevice_client_t* client,
                                  uint64_t version) {
  bool ret = client->rep->syncLogsConfigVersion(version);
  // FIXME: should we ignore LOGS_SECTION_MISSING err?
  if (ret ||
      facebook::logdevice::err == facebook::logdevice::E::LOGS_SECTION_MISSING)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_rename(logdevice_client_t* client, const char* from_path,
                 const char* to_path, HsStablePtr mvar, HsInt cap,
                 logsconfig_status_cb_data_t* data) {
  std::string from_path_ = from_path;
  std::string to_path_ = to_path;
  auto cb = [data, mvar, cap](facebook::logdevice::Status st, uint64_t version,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->version = version;
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->rename(from_path_, to_path_, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

/**
 * This sets either a LogGroup or LogsDirectory attributes to the supplied
 * attributes object. If the path refers to directory, all child directories
 * and log groups will be updated accordingly.
 *
 * @return  0 if the request was successfuly scheduled, -1 otherwise.
 *                      sets err to one of:
 *                        E::INVALID_ATTRIBUTES After applying the parent
 *                                              attributes and the supplied
 *                                              attributes, the resulting
 *                                              attributes are not valid.
 *                        E::NOTFOUND the path supplied doesn't exist.
 *                        E::TIMEDOUT Operation timed out.
 *                        E::ACCESS you don't have permissions to
 *                                  mutate the logs configuration.
 */
facebook::logdevice::Status
ld_client_set_attributes(logdevice_client_t* client, const char* path,
                         LogAttributes* attrs, HsStablePtr mvar, HsInt cap,
                         logsconfig_status_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [data, cap, mvar](facebook::logdevice::Status st, uint64_t version,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->version = version;
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->setAttributes(path, *attrs, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

HsInt ld_client_set_log_group_range(logdevice_client_t* client,
                                    const char* path, c_logid_t start,
                                    c_logid_t end, HsStablePtr mvar, HsInt cap,
                                    logsconfig_status_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [data, cap, mvar](facebook::logdevice::Status st, uint64_t version,
                              const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->version = version;
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  return client->rep->setLogGroupRange(path_, std::make_pair(logid_t(start), logid_t(end)), cb);
}

// ----------------------------------------------------------------------------

[[deprecated]] facebook::logdevice::Status ld_client_make_loggroup_sync(
    logdevice_client_t* client, const char* path, const c_logid_t start_logid,
    const c_logid_t end_logid, LogAttributes* attrs, bool mk_intermediate_dirs,
    logdevice_loggroup_t** loggroup_result) {
  std::unique_ptr<LogGroup> loggroup = nullptr;
  auto start = facebook::logdevice::logid_t(start_logid);
  auto end = facebook::logdevice::logid_t(end_logid);
  std::string reason;

  loggroup = client->rep->makeLogGroupSync(
      path, std::make_pair(start, end), *attrs, mk_intermediate_dirs, &reason);
  if (loggroup) {
    logdevice_loggroup_t* result = new logdevice_loggroup_t;
    result->rep = std::move(loggroup);
    *loggroup_result = result;
    return facebook::logdevice::E::OK;
  }
  std::cerr << "-> ld_client_make_loggroup_sync error: " << reason << "\n";
  return facebook::logdevice::err;
}

[[deprecated]] facebook::logdevice::Status
ld_client_get_loggroup_sync(logdevice_client_t* client, const char* path,
                            logdevice_loggroup_t** loggroup_result) {
  std::unique_ptr<LogGroup> loggroup = nullptr;
  std::string path_ = path;
  loggroup = client->rep->getLogGroupSync(path_);
  if (loggroup) {
    logdevice_loggroup_t* result = new logdevice_loggroup_t;
    result->rep = std::move(loggroup);
    *loggroup_result = result;
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
}

[[deprecated]] facebook::logdevice::Status
ld_client_remove_loggroup_sync(logdevice_client_t* client, const char* path,
                               uint64_t* version) {
  std::string path_ = path;
  bool ret = client->rep->removeLogGroupSync(path_, version);
  if (ret)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

[[deprecated]] facebook::logdevice::Status
ld_client_make_directory_sync(logdevice_client_t* client, const char* path,
                              bool mk_intermediate_dirs, LogAttributes* attrs,
                              logdevice_logdirectory_t** logdir_ret) {
  std::unique_ptr<LogDirectory> directory = nullptr;
  std::string reason;
  directory = client->rep->makeDirectorySync(path, mk_intermediate_dirs, *attrs,
                                             &reason);
  if (directory) {
    logdevice_logdirectory_t* result = new logdevice_logdirectory_t;
    result->rep = std::move(directory);
    *logdir_ret = result;
    return facebook::logdevice::E::OK;
  }
  std::cerr << "-> ld_client_make_directory_sync error: " << reason << "\n";
  return facebook::logdevice::err;
}

[[deprecated]] facebook::logdevice::Status
ld_client_get_directory_sync(logdevice_client_t* client, const char* path,
                             logdevice_logdirectory_t** logdir_result) {
  std::unique_ptr<LogDirectory> logdir = client->rep->getDirectorySync(path);
  if (logdir) {
    logdevice_logdirectory_t* result = new logdevice_logdirectory_t;
    result->rep = std::move(logdir);
    *logdir_result = result;
    return facebook::logdevice::E::OK;
  }
  return facebook::logdevice::err;
}

[[deprecated]] facebook::logdevice::Status
ld_client_remove_directory_sync(logdevice_client_t* client, const char* path,
                                bool recursive, uint64_t* version) {
  bool ret = client->rep->removeDirectorySync(path, recursive, version);
  if (ret)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

// ----------------------------------------------------------------------------
} // end extern "C"
