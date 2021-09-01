#include "hs_cpp_lib.h"

// TODO
template <typename Map, typename KT, typename VT, typename Function>
void cppMapToHs(const Map& map, Function&& extract_value,
                // return values
                size_t* len, KT** keys_ptr, VT** values_ptr,
                std::vector<KT>** keys_, std::vector<VT>** values_) {
  *len = map.size();
  std::vector<KT>* keys = new std::vector<KT>;
  keys.reserve(*len);
  std::vector<VT>* values = new std::vector<VT>;
  values.reserve(*len);

  for (const auto& [key, value] : map) {
    keys->push_back(key);
    values->push_back(extract_value ? extract_value(std::move(value)) : value);
  }

  *keys_ptr = keys->data();
  *values_ptr = values->data();
  *keys_ = keys;
  *values_ = values;
}

// ----------------------------------------------------------------------------

extern "C" {
// ----------------------------------------------------------------------------

std::string* hs_cal_std_string_off(std::string* str, HsInt idx) {
  return str + idx;
}

#define VECTOR_SIZE(NAME, VEC_TYPE)                                            \
  HsInt get_size_##NAME(const VEC_TYPE& vec) { return vec.size(); }

#define PEEK_VECTOR(NAME, VEC_TYPE, VAL_TYPE)                                  \
  void peek_##NAME(const VEC_TYPE& vec, HsInt len, VAL_TYPE* vals) {           \
    assert(("peek_##NAME: size mismatch!", len == vec.size()));                \
    for (int i = 0; i < len; i++) {                                            \
      (vals)[i] = vec[i];                                                      \
    }                                                                          \
  }

#define VECTOR_OFFSET(NAME, VAL_TYPE)                                          \
  VAL_TYPE* cal_offset_##NAME(VAL_TYPE* current, HsInt offset) {               \
    return current + offset;                                                   \
  }

VECTOR_SIZE(vec_of_double, std::vector<double>);
VECTOR_OFFSET(vec_of_folly_small_vec_of_double,
              folly::small_vector<double COMMA 4>);

VECTOR_SIZE(folly_small_vec_of_double, folly::small_vector<double COMMA 4>);
PEEK_VECTOR(folly_small_vec_of_double, folly::small_vector<double COMMA 4>,
            double);

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
