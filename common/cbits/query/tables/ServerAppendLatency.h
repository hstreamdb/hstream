#pragma once

#include "AdminCommandTable.h"

namespace hstream { namespace client { namespace query { namespace tables {

class ServerAppendLatency : public AdminCommandTable {
public:
  explicit ServerAppendLatency(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "server_append_latency"; }
  std::string getDescription() override {
    return "For each server node, reports the estimated percentiles latency of "
           "server appends";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"p50", ldquery::DataType::BIGINT, "p50"},
        {"p75", ldquery::DataType::BIGINT, "p75"},
        {"p95", ldquery::DataType::BIGINT, "p95"},
        {"p99", ldquery::DataType::BIGINT, "p99"},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats server_histogram append_latency "
                       "-p 0.5 -p 0.75 -p 0.95 -p 0.99");
  }
};

}}}} // namespace hstream::client::query::tables
