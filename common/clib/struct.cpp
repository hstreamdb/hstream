#include "hs_cpp_lib.h"

// ----------------------------------------------------------------------------

#define CAL_OFFSET(NAME, VAL_TYPE)                                             \
  VAL_TYPE* cal_offset_##NAME(VAL_TYPE* current, HsInt offset) {               \
    return current + offset;                                                   \
  }

#define VECTOR_SIZE(NAME, VEC_TYPE)                                            \
  HsInt get_size_##NAME(const VEC_TYPE* vec) { return vec->size(); }

#define PEEK_VECTOR(NAME, VEC_TYPE, VAL_TYPE)                                  \
  void peek_##NAME(const VEC_TYPE* vec, HsInt len, VAL_TYPE* vals) {           \
    assert(("peek_##NAME: size mismatch!", len == vec->size()));               \
    for (int i = 0; i < len; i++) {                                            \
      (vals)[i] = (*vec)[i];                                                   \
    }                                                                          \
  }

extern "C" {
// ----------------------------------------------------------------------------

// Unfortunately, there is no generic in c

CAL_OFFSET(std_string, std::string);
CAL_OFFSET(vec_of_folly_small_vec_of_double,
           folly::small_vector<double COMMA 4>);

VECTOR_SIZE(vec_of_double, std::vector<double>);
VECTOR_SIZE(folly_small_vec_of_double, folly::small_vector<double COMMA 4>);

PEEK_VECTOR(folly_small_vec_of_double, folly::small_vector<double COMMA 4>,
            double);

// ----------------------------------------------------------------------------

#define DEL_FUNCTION(NAME, TYPE)                                               \
  void delete_##NAME(TYPE* p) { delete p; }

DEL_FUNCTION(string, std::string);
DEL_FUNCTION(vector_of_string, std::vector<std::string>);
DEL_FUNCTION(vector_of_int64, std::vector<int64_t>);
DEL_FUNCTION(std_vec_of_folly_small_vec_of_double,
             std::vector<folly::small_vector<double COMMA 4>>);

// ----------------------------------------------------------------------------
// Extern End
}
