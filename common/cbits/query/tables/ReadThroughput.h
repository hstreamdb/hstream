#pragma once

#include "AdminCommandTable.h"

namespace hstream { namespace client { namespace query { namespace tables {

class ReadThroughput : public AdminCommandTable {
public:
  explicit ReadThroughput(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "read_throughput"; }
  std::string getDescription() override {
    return "For each server node, reports the estimated per-stream read "
           "throughput over various time periods.";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"stream_name", ldquery::DataType::TEXT, "The name of the stream."},
        {"throughput_15min", ldquery::DataType::BIGINT,
         "Throughput average in the past 15 minutes."},
        {"throughput_30min", ldquery::DataType::BIGINT,
         "Throughput average in the past 30 minutes."},
        {"throughput_60min", ldquery::DataType::BIGINT,
         "Throughput average in the past 60 minutes."},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string(
        "server stats reads --intervals 15min --intervals 30min --intervals 60min");
  }
};

}}}} // namespace hstream::client::query::tables
