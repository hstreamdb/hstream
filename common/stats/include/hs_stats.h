#pragma once

#include <hs_common.h>
#include <hs_cpp_lib.h>
#include <logdevice/common/debug.h>

#include "cbits/stats/Stats.h"

using namespace hstream::common;

extern "C" {
// ----------------------------------------------------------------------------

StatsHolder* new_stats_holder(HsBool is_server);

void delete_stats_holder(StatsHolder* s);

// ----------------------------------------------------------------------------
} /* end extern "C" */
