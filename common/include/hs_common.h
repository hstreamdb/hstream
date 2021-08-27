#pragma once

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

struct Stats;
struct StatsHolder;

Stats* new_stats();
void delete_stats(Stats* s);

// ----------------------------------------------------------------------------
#ifdef __cplusplus
} /* end extern "C" */
#endif
