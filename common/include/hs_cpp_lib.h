#pragma once

#include <HsFFI.h>

#include <cstddef>
#include <functional>
#include <map>
#include <vector>

#include <folly/small_vector.h>

#ifndef COMMA
#define COMMA ,
#endif

// ----------------------------------------------------------------------------
// CPP Struct to Haskell binding format

template <typename Map, typename KT, typename VT, typename FunctionK,
          typename FunctionV>
void cppMapToHs(const Map& map, FunctionK extract_key, FunctionV extract_value,
                // return values
                HsInt* len, KT** keys_ptr, VT** values_ptr,
                std::vector<KT>** keys_, std::vector<VT>** values_) {
  *len = map.size();
  std::vector<KT>* keys = new std::vector<KT>;
  keys->reserve(*len);
  std::vector<VT>* values = new std::vector<VT>;
  values->reserve(*len);

  for (const auto& [key, value] : map) {
    if constexpr (std::is_null_pointer_v<FunctionK>) {
      keys->push_back(key);
    } else {
      keys->push_back(extract_key(std::move(key)));
    }
    if constexpr (std::is_null_pointer_v<FunctionV>) {
      values->push_back(value);
    } else {
      values->push_back(extract_value(std::move(value)));
    }
  }

  *keys_ptr = keys->data();
  *values_ptr = values->data();
  *keys_ = keys;
  *values_ = values;
}

// ----------------------------------------------------------------------------
