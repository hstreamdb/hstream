#pragma once

#include <HsFFI.h>

#include <cstddef>
#include <cstdint>
#include <iostream>

// ----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif
// ----------------------------------------------------------------------------
// Utils

void setup_fatal_signal_handler();

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
