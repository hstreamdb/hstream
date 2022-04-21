#pragma once

#include "AdminCommandTable.h"

namespace hstream { namespace client { namespace query { namespace tables {

class AppendThroughput : public AdminCommandTable {
public:
  explicit AppendThroughput(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "append_throughput"; }
  std::string getDescription() override {
    return "For each server node, reports the estimated per-stream append "
           "throughput over various time periods.";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"stream_name", ldquery::DataType::TEXT, "The name of the stream."},
        {"appends_1min", ldquery::DataType::BIGINT,
         "Throughput average in the past 1 minute."},
        {"appends_5min", ldquery::DataType::BIGINT,
         "Throughput average in the past 5 minutes."},
        {"appends_10min", ldquery::DataType::BIGINT,
         "Throughput average in the past 10 minutes."},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats stream appends --intervals 1min "
                       "--intervals 5min --intervals 10min");
  }
};

}}}} // namespace hstream::client::query::tables
