#include "hs_logdevice.h"
#include <logdevice/ops/ldquery/Errors.h>
#include <logdevice/ops/ldquery/LDQuery.h>

using namespace facebook::logdevice::ldquery;

// TODO: use common/cbits/query.cpp instead

extern "C" {
// ----------------------------------------------------------------------------

LDQuery* new_ldquery(const char* config_path, int64_t command_timeout,
                     bool use_ssl) {
  return new LDQuery(config_path, std::chrono::milliseconds(command_timeout),
                     use_ssl);
}

void delete_ldquery(LDQuery* ldq) { delete ldq; };

void ldquery_show_tables(
    LDQuery* ldq, size_t* len,
    //
    std::string** tables_name_val,
    std::vector<std::string>** tables_name_, // for deletion on haskell side
    //
    std::string** tables_description_val,
    std::vector<std::string>** tables_description_) {

  std::vector<TableMetadata> tables = ldq->getTables();

  if (!tables.empty()) {
    // NOTE: do not forget to free this
    auto tables_name = new std::vector<std::string>;
    auto tables_description = new std::vector<std::string>;

    for (auto& table : tables) {
      tables_name->push_back(table.name);
      tables_description->push_back(table.description);
    }

    *len = tables_name->size();
    *tables_name_val = tables_name->data();
    *tables_name_ = tables_name;
    *tables_description_val = tables_description->data();
    *tables_description_ = tables_description;
  } else {
    *len = 0;
  }
}

void ldquery_show_table_columns(
    LDQuery* ldq, const char* table_name, size_t* len,
    //
    std::string** cols_name_val, std::vector<std::string>** cols_name_,
    //
    std::string** cols_type_val, std::vector<std::string>** cols_type_,
    //
    std::string** cols_desc_val, std::vector<std::string>** cols_desc_) {

  std::vector<TableMetadata> tables = ldq->getTables();
  auto result = std::find_if(tables.begin(), tables.end(),
                             [&table_name](TableMetadata table) {
                               return table.name == std::string(table_name);
                             });
  if (result != std::end(tables)) {
    auto cols_name = new std::vector<std::string>;
    auto cols_type = new std::vector<std::string>;
    auto cols_desc = new std::vector<std::string>;
    for (auto& col : result->columns) {
      cols_name->push_back(col.name);
      cols_type->push_back(col.type_as_string());
      cols_desc->push_back(col.description);
    }
    *len = cols_name->size();
    *cols_name_val = cols_name->data();
    *cols_name_ = cols_name;
    *cols_type_val = cols_type->data();
    *cols_type_ = cols_type;
    *cols_desc_val = cols_desc->data();
    *cols_desc_ = cols_desc;
  } else {
    *len = 0;
  }
}

void ldquery_query(LDQuery* ldq, const char* query, size_t* results_len,
                   LDQuery::QueryResults** results_val, char** exinfo) {
  try {
    LDQuery::QueryResults results_ = ldq->query(query);
    if (!results_.empty()) {
      LDQuery::QueryResults* results = new LDQuery::QueryResults;
      *results = results_;
      *results_len = results->size();
      *results_val = results;
    } else {
      *results_len = 0;
    }
    *exinfo = NULL;
  } catch (LDQueryError& e) {
    *results_len = 0;
    *exinfo = strdup(e.what());
  }
}

// TODO: use common/cbits/query.cpp instead
void delete_query_results_(LDQuery::QueryResults* rs) { delete rs; }

#define GET_VEC_INDEX_VAL(FUN_NAME, FROM_TYPE, ELE_SIZE_FUN, ELE_DATA_FUN,     \
                          ELE_TYPE)                                            \
  void FUN_NAME(FROM_TYPE* datas, HsInt index, size_t* len,                    \
                ELE_TYPE** ret_val) {                                          \
    auto& data = (*datas)[index]; /* we do not check the bound */              \
    *len = data.ELE_SIZE_FUN();                                                \
    *ret_val = data.ELE_DATA_FUN();                                            \
  }

GET_VEC_INDEX_VAL(queryResults__headers_, LDQuery::QueryResults, headers.size,
                  headers.data, std::string)
GET_VEC_INDEX_VAL(queryResults__cols_max_size_, LDQuery::QueryResults,
                  cols_max_size.size, cols_max_size.data, size_t)

size_t queryResults_rows_len(LDQuery::QueryResults* datas, HsInt index) {
  return (*datas)[index].rows.size();
}

void queryResults_rows_val(LDQuery::QueryResults* datas, HsInt index, HsInt row,
                           size_t* len, std::string** row_val) {
  auto& data = (*datas)[index].rows[row];
  *len = data.size();
  *row_val = data.data();
}

uint64_t queryResults__metadata_contacted_nodes_(LDQuery::QueryResults* datas,
                                                 HsInt index) {
  return (*datas)[index].metadata.contacted_nodes;
}

uint64_t queryResults__metadata_latency_(LDQuery::QueryResults* datas,
                                         HsInt index) {
  return (*datas)[index].metadata.latency;
}

void queryResults__metadata_failures_(
    LDQuery::QueryResults* datas, HsInt index,
    //
    size_t* len,
    //
    int** key_val, std::vector<int>** key_,
    //
    std::string** address_val, std::vector<std::string>** address_,
    //
    std::string** failure_reason_val,
    std::vector<std::string>** failure_reason_) {

  auto& data = (*datas)[index].metadata.failures;
  if (!data.empty()) {
    auto key = new std::vector<int>;
    auto address = new std::vector<std::string>;
    auto failure_reason = new std::vector<std::string>;
    for (const auto& [k, v] : data) {
      key->push_back(k);
      address->push_back(v.address);
      failure_reason->push_back(v.failure_reason);
    }
    *len = data.size();
    *key_val = key->data();
    *key_ = key;
    *address_val = address->data();
    *address_ = address;
    *failure_reason_val = failure_reason->data();
    *failure_reason_ = failure_reason;
  } else {
    *len = 0;
  }
}

// ----------------------------------------------------------------------------
}
