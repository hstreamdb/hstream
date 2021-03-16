#include "hs_logdevice.h"

extern "C" {

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

facebook::logdevice::Status
ld_client_remove_loggroup_sync(logdevice_client_t* client, const char* path,
                               uint64_t* version) {
  std::string path_ = path;
  bool ret = client->rep->removeLogGroupSync(path_, version);
  if (ret)
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

uint64_t ld_loggroup_get_version(logdevice_loggroup_t* group) {
  return group->rep->version();
}

void* free_logdevice_loggroup(logdevice_loggroup_t* group) { delete group; }

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

void* free_logdevice_logdirectory(logdevice_logdirectory_t* dir) { delete dir; }

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

// ----------------------------------------------------------------------------
} // end extern "C"
