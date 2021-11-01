#pragma once

#include <HsFFI.h>

#include <cstddef>
#include <cstdint>
#include <iostream>

#include "cbits/stats/Stats.h"

// ----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif
// ----------------------------------------------------------------------------
// Utils

void setup_sigsegv_handler();

// ----------------------------------------------------------------------------
// Stats
//
// See: cbits/stats.cpp

// ----------------------------------------------------------------------------
// Query
//
// See: cbits/query.cpp

// ----------------------------------------------------------------------------
#ifdef __cplusplus
} /* end extern "C" */
#endif
