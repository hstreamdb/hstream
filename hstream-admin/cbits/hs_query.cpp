#include "cbits/query/Query.h"

namespace query = hstream::client::query;

extern "C" {
// ----------------------------------------------------------------------------

query::Query* new_hstream_query(const char* addr) {
  return new query::Query(std::string(addr));
}

void delete_hstream_query(query::Query* q) { delete q; }

// ----------------------------------------------------------------------------
} // End extern "C"
