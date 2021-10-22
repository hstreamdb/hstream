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

void setup_sigsegv_handler();

// ----------------------------------------------------------------------------
// Stats

struct StatsHolder;

// ----------------------------------------------------------------------------
// Query
//
// See: cbits/query.cpp


// ----------------------------------------------------------------------------
#ifdef __cplusplus
} /* end extern "C" */
#endif
