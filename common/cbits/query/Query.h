#pragma once

#include <logdevice/ops/ldquery/QueryBase.h>

#include "cbits/query/Table.h"

namespace ldquery = facebook::logdevice::ldquery;

namespace hstream { namespace client { namespace query {

class Query : public ldquery::QueryBase {
public:
  explicit Query(std::string addr);
  ~Query();

private:
  void registerTables();
  ldquery::ActiveQueryMetadata& getActiveQuery() const;
  void resetActiveQuery();

  std::shared_ptr<Context> ctx_;
  std::string addr_;
};

}}} // namespace hstream::client::query
