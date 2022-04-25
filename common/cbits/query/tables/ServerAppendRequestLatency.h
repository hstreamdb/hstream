#pragma once

#include "AdminCommandTable.h"

namespace hstream { namespace client { namespace query { namespace tables {

class ServerAppendRequestLatency : public AdminCommandTable {
public:
  explicit ServerAppendRequestLatency(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "server_append_request_latency"; }
  std::string getDescription() override {
    return "For each server node, reports the estimated percentiles latency of "
           "append request";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"p50", ldquery::DataType::BIGINT,
         ""},
        {"p75", ldquery::DataType::BIGINT,
         ""},
        {"p95", ldquery::DataType::BIGINT,
         ""},
        {"p99", ldquery::DataType::BIGINT,
         ""},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats server_histogram append_request_latency "
                       "-p 0.5 -p 0.75 -p 0.95 -p 0.99");
  }
};

}}}} // namespace hstream::client::query::tables
