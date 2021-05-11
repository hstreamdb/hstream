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
int get_replicationFactor(LogAttributes* attrs) {
  return attrs->replicationFactor().value();
}

std::string* describe_log_maxWritesInFlight(LogAttributes* attrs) {
  return new_hs_std_string(attrs->maxWritesInFlight().describe());
}

// ----------------------------------------------------------------------------
// LogConfigType: LogGroup

facebook::logdevice::Status ld_client_make_loggroup_sync(
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

facebook::logdevice::Status
ld_client_make_loggroup(logdevice_client_t* client, const char* path,
                        const c_logid_t start_logid, const c_logid_t end_logid,
                        LogAttributes* attrs, bool mk_intermediate_dirs,
                        HsStablePtr mvar, HsInt cap,
                        make_loggroup_cb_data_t* data) {
  std::string path_ = path;
  auto start = facebook::logdevice::logid_t(start_logid);
  auto end = facebook::logdevice::logid_t(end_logid);
  auto cb = [&](facebook::logdevice::Status st,
                std::unique_ptr<LogGroup> loggroup_ptr,
                const std::string& failure_reason) {
    if (data) {
      data->st = static_cast<c_error_code_t>(st);
      data->loggroup = new logdevice_loggroup_t;
      data->loggroup->rep = std::move(loggroup_ptr);
      data->failure_reason = strdup(failure_reason.c_str());
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  int ret = client->rep->makeLogGroup(path_, std::make_pair(start, end),
                                      *attrs, mk_intermediate_dirs, cb);
  if (ret == 0)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
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

void ld_client_get_loggroup(logdevice_client_t* client, const char* path,
                            HsStablePtr mvar, HsInt cap,
                            facebook::logdevice::Status* st_out,
                            logdevice_loggroup_t** loggroup_result) {
  std::string path_ = path;
  auto cb = [&](facebook::logdevice::Status st,
                std::unique_ptr<LogGroup> loggroup_ptr) {
    if (st_out && loggroup_result) {
      *st_out = st;
      *loggroup_result = new logdevice_loggroup_t;
      (*loggroup_result)->rep = std::move(loggroup_ptr);
    }
    hs_try_putmvar(cap, mvar);
    hs_thread_done();
  };
  client->rep->getLogGroup(path_, cb);
}

facebook::logdevice::Status
ld_client_remove_loggroup_sync(logdevice_client_t* client, const char* path,
                               uint64_t* version) {
  std::string path_ = path;
  bool ret = client->rep->removeLogGroupSync(path_, version);
  if (ret)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

facebook::logdevice::Status
ld_client_remove_loggroup(logdevice_client_t* client, const char* path,
                          HsStablePtr mvar, HsInt cap,
                          logsconfig_status_cb_data_t* data) {
  std::string path_ = path;
  auto cb = [&](facebook::logdevice::Status st, uint64_t version,
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

void ld_loggroup_get_range(logdevice_loggroup_t* group, c_logid_t* start,
                           c_logid_t* end) {
  const facebook::logdevice::logid_range_t& range = group->rep->range();
  *start = range.first.val();
  *end = range.second.val();
}

// NOTE: returned null-terminated string should be copied from ffi function.
const char* ld_loggroup_get_name(logdevice_loggroup_t* group) {
  return group->rep->name().c_str();
}

const char* ld_loggroup_get_fully_qualified_name(logdevice_loggroup_t* group) {
  return group->rep->getFullyQualifiedName().c_str();
}

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

facebook::logdevice::Status
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

facebook::logdevice::Status
ld_client_remove_directory_sync(logdevice_client_t* client, const char* path,
                                bool recursive, uint64_t* version) {
  bool ret = client->rep->removeDirectorySync(path, recursive, version);
  if (ret)
    return facebook::logdevice::E::OK;
  return facebook::logdevice::err;
}

void free_logdevice_logdirectory(logdevice_logdirectory_t* dir) { delete dir; }

const char* ld_logdirectory_get_name(logdevice_logdirectory_t* dir) {
  return dir->rep->name().c_str();
}

uint64_t ld_logdirectory_get_version(logdevice_logdirectory_t* dir) {
  return dir->rep->version();
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
  auto cb = [&](facebook::logdevice::Status st, uint64_t version,
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
  auto cb = [&](facebook::logdevice::Status st, uint64_t version,
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

// ----------------------------------------------------------------------------
} // end extern "C"
