#pragma once

// * --------------------------------------------------------------------------
// Note that we can not include Stg.h or Rts.h, so we need to copy the necessary
// parts to this file.

#include <HsFFI.h>
#include <rts/Types.h>

typedef struct CostCentre_ {
  StgInt ccID; // Unique Id, allocated by the RTS

  char* label;
  char* module;
  char* srcloc;

  // used for accumulating costs at the end of the run...
  StgWord64 mem_alloc; // align 8 (Note [struct alignment])
  StgWord time_ticks;

  StgBool is_caf; // true <=> CAF cost centre

  struct CostCentre_* link;
} CostCentre;

typedef struct CostCentreStack_ {
  StgInt ccsID; // unique ID, allocated by the RTS

  CostCentre* cc; // Cost centre at the top of the stack

  struct CostCentreStack_* prevStack; // parent
  struct IndexTable_* indexTable;     // children
  struct CostCentreStack_* root;      // root of stack
  StgWord depth;                      // number of items in the stack

  StgWord64 scc_count; // Count of times this CCS is entered
                       // align 8 (Note [struct alignment])

  StgWord selected; // is this CCS shown in the heap
                    // profile? (zero if excluded via -hc
                    // -hm etc.)

  StgWord time_ticks; // number of time ticks accumulated by
                      // this CCS

  StgWord64 mem_alloc; // mem allocated by this CCS
                       // align 8 (Note [struct alignment])

  StgWord64 inherited_alloc; // sum of mem_alloc over all children
                             // (calculated at the end)
                             // align 8 (Note [struct alignment])

  StgWord inherited_ticks; // sum of time_ticks over all children
                           // (calculated at the end)
} CostCentreStack;

#include <rts/storage/Closures.h>
