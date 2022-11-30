#include "hs_common.h"
#include <logdevice/common/hash.h>

using namespace facebook::logdevice::hashing;

extern "C" {

uint64_t get_allocated_num(uint64_t key, uint64_t buckets) {
  // John Lamping, Eric Veach.
  // A Fast, Minimal Memory, Consistent Hash Algorithm.
  // http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
  return ch(key, buckets);
}
}
