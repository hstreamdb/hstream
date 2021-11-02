#pragma once

#include "AdminCommandTable.h"

namespace hstream { namespace client { namespace query { namespace tables {

class Streams : public AdminCommandTable {
public:
  explicit Streams(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx, AdminCommandTable::Type::JSON_TABLE) {}
  static std::string getName() { return "streams"; }
  std::string getDescription() override {
    return "A table that lists the streams created in the cluster.";
  }
  ldquery::TableColumns getFetchableColumns() const override {
    return {
        {"name", ldquery::DataType::TEXT, "The name of the stream."},
        {"replication_property", ldquery::DataType::TEXT,
         "Replication property configured for this stream."},
    };
  }
  std::string getCommandToSend(ldquery::QueryContext& /*ctx*/) const override {
    return std::string("stream list");
  }
};

}}}} // namespace hstream::client::query::tables
