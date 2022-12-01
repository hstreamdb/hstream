#pragma once

#include "cbits/query/tables/AdminCommandTable.h" // from hstream-common

namespace hstream { namespace client { namespace query { namespace tables {

class SubscriptionThroughput : public AdminCommandTable {
public:
  explicit SubscriptionThroughput(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "subscription_throughput"; }
  std::string getDescription() override {
    return "For each server node, reports the estimated per-stream append "
           "throughput over various time periods.";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"subscription_id", ldquery::DataType::TEXT,
         "The id of the subscription."},
        {"sends_1min", ldquery::DataType::BIGINT,
         "Throughput average in the past 1 minute."},
        {"sends_5min", ldquery::DataType::BIGINT,
         "Throughput average in the past 5 minutes."},
        {"sends_10min", ldquery::DataType::BIGINT,
         "Throughput average in the past 10 minutes."},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("server stats subscription sends --intervals 1min "
                       "--intervals 5min --intervals 10min");
  }
};

}}}} // namespace hstream::client::query::tables
