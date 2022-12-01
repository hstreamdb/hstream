#pragma once

#include "cbits/query/tables/AdminCommandTable.h" // from hstream-common

namespace hstream { namespace client { namespace query { namespace tables {

class AppendFailedCounter : public AdminCommandTable {
public:
  explicit AppendFailedCounter(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "append_failed_counter"; }
  std::string getDescription() override { return "Failed append requests."; }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"stream_name", ldquery::DataType::TEXT, "The name of the stream."},
        {"append_failed", ldquery::DataType::BIGINT,
         "Number of failed appends"},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats stream_counter append_failed");
  }
};

}}}} // namespace hstream::client::query::tables
