#include "hs_cpp_lib.h"

// ----------------------------------------------------------------------------
// StdString

std::string* copy_std_string(std::string&& str) {
  auto value = new std::string;
  *value = str;
  return value;
}

// ----------------------------------------------------------------------------

#define CAL_OFFSET(NAME, VAL_TYPE)                                             \
  VAL_TYPE* cal_offset_##NAME(VAL_TYPE* current, HsInt offset) {               \
    return current + offset;                                                   \
  }

#define GET_SIZE(NAME, VAL_TYPE)                                               \
  HsInt get_size_##NAME(const VAL_TYPE* v) { return v->size(); }

#define DEL_FUNCTION(NAME, TYPE)                                               \
  void delete_##NAME(TYPE* p) { delete p; }

#define PEEK_VECTOR(NAME, VEC_TYPE, VAL_TYPE)                                  \
  void peek_##NAME(const VEC_TYPE* vec, HsInt vals_len, VAL_TYPE* vals) {      \
    assert(("peek_##NAME: size mismatch!", vals_len == vec->size()));          \
    for (int i = 0; i < vals_len; i++) {                                       \
      (vals)[i] = (*vec)[i];                                                   \
    }                                                                          \
  }

extern "C" {
// ----------------------------------------------------------------------------

// Unfortunately, there is no generic in c

GET_SIZE(vec_of_double, std::vector<double>);
GET_SIZE(vec_of_uint64, std::vector<uint64_t>);
GET_SIZE(folly_small_vec_of_double, folly::small_vector<double COMMA 4>);

// Ptr a -> Int -> Ptr a
CAL_OFFSET(std_string, std::string);
CAL_OFFSET(vec_of_uint64, std::vector<uint64_t>);
CAL_OFFSET(folly_small_vec_of_double, folly::small_vector<double COMMA 4>);

PEEK_VECTOR(vec_of_uint64, std::vector<uint64_t>, uint64_t)
PEEK_VECTOR(folly_small_vec_of_double, folly::small_vector<double COMMA 4>,
            double);

DEL_FUNCTION(vector_of_int, std::vector<int>);
DEL_FUNCTION(string, std::string);
DEL_FUNCTION(vector_of_string, std::vector<std::string>);
DEL_FUNCTION(vector_of_int64, std::vector<int64_t>);
DEL_FUNCTION(vector_of_uint64, std::vector<uint64_t>);
DEL_FUNCTION(std_vec_of_folly_small_vec_of_double,
             std::vector<folly::small_vector<double COMMA 4>>);

// ----------------------------------------------------------------------------
// Extern End
}
