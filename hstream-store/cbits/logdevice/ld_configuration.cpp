#include "hs_logdevice.h"

#include <logdevice/common/configuration/InternalLogs.h>

extern "C" {
// ----------------------------------------------------------------------------

bool isInternalLog(c_logid_t logid) {
  return ld::configuration::InternalLogs::isInternal(logid_t(logid));
}

std::string* getInternalLogName(c_logid_t logid) {
  return copy_std_string(
      ld::configuration::InternalLogs::lookupByID(logid_t(logid)));
}

// ----------------------------------------------------------------------------
} // end extern "C"
