#pragma once

#include "cbits/query/tables/AdminCommandTable.h" // from hstream-common

namespace hstream { namespace client { namespace query { namespace tables {

class AppendTotalCounter : public AdminCommandTable {
public:
  explicit AppendTotalCounter(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "append_total_counter"; }
  std::string getDescription() override {
    return "Total append requests server received.";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"stream_name", ldquery::DataType::TEXT, "The name of the stream."},
        {"append_total", ldquery::DataType::BIGINT, "Number of total appends"},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats stream_counter append_total");
  }
};

}}}} // namespace hstream::client::query::tables
