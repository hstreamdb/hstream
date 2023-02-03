#pragma once

#include <logdevice/ops/ldquery/Context.h>
#include <logdevice/ops/ldquery/Table.h>

#include "AdminClient.h" // installed by hstream-client-cpp

namespace ldquery = facebook::logdevice::ldquery;

namespace hstream { namespace client { namespace query {

struct Context : public ldquery::ContextBase {
public:
  explicit Context(std::string& addr) : addr_(addr) {}

  std::shared_ptr<AdminCommandClient> getClient() {
    if (!adminCommandClient_) {
      adminCommandClient_ = std::make_shared<AdminCommandClient>(addr_);
    }
    return adminCommandClient_;
  }

private:
  std::string addr_{""};
  std::shared_ptr<AdminCommandClient> adminCommandClient_;
};

class Table : public ldquery::TableBase {
public:
  explicit Table(std::shared_ptr<Context> ctx) : ctx_(ctx) {}

  const Context& getContext() const { return *ctx_; }

protected:
  std::shared_ptr<Context> ctx_;

private:
  // Map a column name to its position.
  std::unordered_map<ldquery::ColumnName, int> nameToPosMap_;
};

}}} // namespace hstream::client::query
