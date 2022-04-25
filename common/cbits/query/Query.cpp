#include "Query.h"

#include "cbits/query/tables/AppendThroughput.h"
#include "cbits/query/tables/ReadThroughput.h"
#include "cbits/query/tables/ServerAppendRequestLatency.h"
#include "cbits/query/tables/Streams.h"

namespace hstream { namespace client { namespace query {

void Query::registerTables() {
  table_registry_.registerTable<tables::AppendThroughput>(ctx_);
  table_registry_.registerTable<tables::ReadThroughput>(ctx_);
  table_registry_.registerTable<tables::ServerAppendRequestLatency>(ctx_);
  table_registry_.registerTable<tables::Streams>(ctx_);

  if (table_registry_.attachTables(db_) != 0) {
    throw facebook::logdevice::ConstructorFailed();
  }

  setCacheTTL(cache_ttl_);
}

Query::Query(std::string addr) : addr_(addr), QueryBase() {
  ctx_ = std::make_shared<Context>(addr);

  registerTables();
}

Query::~Query() {}

ldquery::ActiveQueryMetadata& Query::getActiveQuery() const {
  return ctx_->activeQueryMetadata;
}

void Query::resetActiveQuery() { return ctx_->resetActiveQuery(); }

}}} // namespace hstream::client::query
