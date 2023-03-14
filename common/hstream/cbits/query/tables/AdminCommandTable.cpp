#include "AdminCommandTable.h"

#include <thread>

#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/json.h>
#include <gason/gason.h>
#include <logdevice/common/debug.h>

#include "HStream/Server/HStreamApi.pb.h"
#include "cbits/checks.h"

using hstream::server::ServerNode;
using std::chrono::steady_clock;

namespace hstream { namespace client { namespace query {

namespace {
struct ColumnAndDataType {
  ldquery::Column* data = nullptr;
  ldquery::DataType type;
};
} // namespace

void AdminCommandTable::init() {
  int i = 0;
  for (const auto& c : getFetchableColumns()) {
    nameToPosMap_[c.name] = i++;
  }
}

std::shared_ptr<ldquery::TableData>
AdminCommandTable::getData(ldquery::QueryContext& ctx) {
  // First, refill the cache if necessary.
  refillCache(ctx);
  hs_check(admin_cmd_cache_.count(ctx.used_constraints));

  Data& cached = admin_cmd_cache_[ctx.used_constraints]->get();

  // Then, check if we can serve the data from an index.
  auto c = findIndexableConstraint(ctx);
  if (!c.has_value()) {
    // If we are here, we could not fetch data from an index. Return everything
    // we have, SQLite will filter it.
    return cached.data;
  }

  auto& expr = c.value().second->expr;
  if (!expr.has_value()) {
    // TODO: if the constraint compares a column with "null", we do
    // not check the index because the index currently does not support indexing
    // null values.
    return cached.data;
  }

  // There is an index we can use. If we have not built the index yet, do it
  // now.
  auto& indices = cached.indices;
  if (indices.find(c.value().first) == indices.end()) {
    buildIndexForConstraint(cached, c.value().first, c.value().second);
  }

  if (indices.find(c.value().first) == indices.end()) {
    // No index was created, there is no data.
    return std::make_shared<ldquery::TableData>();
  }

  // Return the data stored in the index.
  auto it = indices[c.value().first].find(expr.value());
  if (it == indices[c.value().first].end()) {
    // There is no data found that matches the constraint.
    return std::make_shared<ldquery::TableData>();
  }

  ldquery::TableData result;
  for (const auto& cached_col : cached.data->cols) {
    ldquery::Column& result_col = result.cols[cached_col.first];
    result_col.reserve(it->second.size());
    for (size_t idx : it->second) {
      result_col.push_back(cached_col.second[idx]);
    }
  }
  return std::make_shared<ldquery::TableData>(std::move(result));
}

bool AdminCommandTable::allowServerSideFiltering() const {
  // Allow server side filtering if the user did not call
  // QueryBase::enableServerSideFiltering(true) and if we have not already done
  // more than MAX_FETCHES roundtrips to the server.
  return enable_server_side_filtering_ && num_fetches_ <= MAX_FETCHES;
}

AdminCommandTable::MatchResult
AdminCommandTable::nodeMatchesConstraint(AdminCommandTable::node_id_t nid,
                                         const ldquery::Constraint& c) {
  if (!c.expr.has_value()) {
    return MatchResult::UNUSED;
  }
  std::string expr = c.expr.value();
  int node_id = folly::to<int>(expr);
  if (c.op == SQLITE_INDEX_CONSTRAINT_EQ) {
    if (nid != node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_LT) {
    if (nid >= node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_GT) {
    if (nid <= node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_LE) {
    if (nid > node_id) {
      return MatchResult::NO_MATCH;
    }
  } else if (c.op == SQLITE_INDEX_CONSTRAINT_GE) {
    if (nid < node_id) {
      return MatchResult::NO_MATCH;
    }
  } else {
    return MatchResult::UNUSED;
  }
  return MatchResult::MATCH;
}

ldquery::TableData
AdminCommandTable::aggregate(std::vector<ldquery::TableData> results) const {
  ldquery::TableData result;

  size_t total_rows = 0;
  for (const auto& src : results) {
    total_rows += src.numRows();
  }

  size_t rows_so_far = 0;
  for (auto& src : results) {
    size_t rows_in_current = 0;
    for (const auto& kv : src.cols) {
      const ldquery::ColumnName& name = kv.first;
      const ldquery::Column& c = kv.second;

      hs_check(rows_in_current == 0 || rows_in_current == c.size());
      rows_in_current = c.size();

      ldquery::Column& res = result.cols[name];
      if (res.empty()) { // inserted a new column
        res.reserve(total_rows);
      }
      // If some of the `results` didn't have this column, fill corresponding
      // rows with nulls.
      res.resize(rows_so_far);
      // Copy the data.
      res.insert(res.end(), std::make_move_iterator(c.begin()),
                 std::make_move_iterator(c.end()));
    }
    rows_so_far += rows_in_current;
  }
  hs_check(rows_so_far == total_rows);

  for (auto& kv : result.cols) {
    // If the last of the `results` didn't contain some of the columns, fill
    // them with nulls.
    kv.second.resize(total_rows);
  }

  return result;
}

void AdminCommandTable::newQuery() {
  // We are beginning a new query. Clear the cache according to TTLs.

  auto it = admin_cmd_cache_.begin();
  while (it != admin_cmd_cache_.end()) {
    if (!it->second->isValid(cache_ttl_)) {
      it = admin_cmd_cache_.erase(it);
    } else {
      ++it;
    }
  }

  num_fetches_ = 0;
}

void AdminCommandTable::setCacheTTL(std::chrono::seconds ttl) {
  cache_ttl_ = ttl;
}

bool AdminCommandTable::dataIsInCacheForUsedConstraints(
    const ldquery::ConstraintMap& used_constraints) {
  auto it = admin_cmd_cache_.find(used_constraints);
  return it != admin_cmd_cache_.end();
}

void AdminCommandTable::buildIndexForConstraint(
    Data& data, int col, const ldquery::Constraint* ctx) {
  hs_check(ctx->op == SQLITE_INDEX_CONSTRAINT_EQ);
  hs_check(col < getColumnsImpl().size());
  const ldquery::ColumnName& col_name = getColumnsImpl()[col].name;

  ld_info("Building index for column `%s`...", col_name.c_str());
  steady_clock::time_point tstart = steady_clock::now();

  data.indices[col]; // create the index even if it's going to be empty

  if (!data.data->cols.count(col_name)) {
    // All values are null.
  } else {
    const ldquery::Column& col_vec = data.data->cols.at(col_name);
    for (size_t i = 0; i < col_vec.size(); ++i) {
      const ldquery::ColumnValue& v = col_vec[i];
      // We don't fill the index with null values.
      if (v.has_value()) {
        data.indices[col][v.value()].push_back(i);
      }
    }
  }

  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info("Building index for column `%s` took %.1fs", col_name.c_str(),
          duration);
}

folly::Optional<std::pair<int, const ldquery::Constraint*>>
AdminCommandTable::findIndexableConstraint(const ldquery::QueryContext& ctx) {
  for (const auto& e : ctx.constraints) {
    const auto col = e.first;
    const auto& c = e.second;
    for (auto& constraint : c.constraints_) {
      if (constraint.op != SQLITE_INDEX_CONSTRAINT_EQ) {
        // TODO: we discard constraints for which the operator is
        // not equality because we currently only support indexes for equality.
        continue;
      }
      return std::make_pair(col, &constraint);
    }
  }
  return folly::none;
}

class NameNormalizer {
  struct both_underscores {
    bool operator()(char a, char b) const { return a == '_' && b == '_'; }
  };

public:
  static void normalize(std::string& name) {
    // Replacing all non-alphanumerics with _
    auto strip_chars = [](char ch) {
      if (ch >= '0' && ch <= '9') {
        return ch;
      }
      if (ch >= 'a' && ch <= 'z') {
        return ch;
      }
      if (ch >= 'A' && ch <= 'Z') {
        return (char)::tolower(ch);
      }
      return '_';
    };
    std::transform(name.begin(), name.end(), name.begin(), strip_chars);

    // trim trailing trash
    size_t endpos = name.find_last_not_of("_");
    if (std::string::npos != endpos) {
      name = name.substr(0, endpos + 1);
    }
    // trim leading trash
    size_t startpos = name.find_first_not_of("_");
    if (std::string::npos != startpos) {
      name = name.substr(startpos);
    }
    // replace multiple consecutive underscores
    name.erase(std::unique(name.begin(), name.end(), both_underscores()),
               name.end());
  }
};

ldquery::TableColumns AdminCommandTable::getColumns() const {
  return getColumnsImpl();
}

const ldquery::TableColumns& AdminCommandTable::getColumnsImpl() const {
  if (columns_.empty()) {
    columns_ = this->getFetchableColumns();
    columns_.insert(columns_.begin(), {"node_id", ldquery::DataType::INTEGER,
                                       "Node ID this row is for."});
  }

  return columns_;
}

PartialTableData AdminCommandTable::jsonToTableData(std::string json) const {
  char* endptr;
  steady_clock::time_point tstart = steady_clock::now();
  JsonValue value;
  JsonAllocator allocator;
  int status = jsonParse((char*)json.data(), &endptr, &value, allocator);
  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  if (status != JSON_OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Cannot parse json result");

    return PartialTableData{folly::none, false, "JSON_PARSE_ERROR"};
  }

  // Descriptions of the columns in the json result. If `data` is nullptr,
  // the column is not present in getFetchableColumns(), so we should ignore it.
  std::vector<ColumnAndDataType> columns_present;
  ldquery::TableData results;

  auto parse_headers = [&](const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_ARRAY) {
      ld_error("Expecting array for headers");
      return false;
    }
    for (const auto& i : o) {
      if (i->value.getTag() != JsonTag::JSON_STRING) {
        ld_error("Expecting string for column name");
        return false;
      }
      std::string name = i->value.toString();
      NameNormalizer::normalize(name);

      auto it = nameToPosMap_.find(name);
      if (it == nameToPosMap_.end()) {
        ld_warning("Unable to find the cloumn: %s", name.c_str());
        // Tell the row parser to ignore this column.
        columns_present.emplace_back();
      } else {
        const int col_pos = it->second;
        hs_check(getFetchableColumns()[col_pos].name == name);
        ColumnAndDataType column_info;
        column_info.type = getFetchableColumns()[col_pos].type;
        column_info.data = &results.cols[name];
        columns_present.push_back(column_info);
      }
    }
    return true;
  };

  auto parse_row = [&](size_t row_idx, const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_ARRAY) {
      ld_error("Expecting array for row");
      return false;
    }
    size_t col = 0;
    for (const auto& i : o) {
      hs_check(i);
      if (col >= columns_present.size()) {
        ++col;
        // The error will be reported after the loop.
        continue;
      }
      auto& column_info = columns_present[col++];
      if (column_info.data == nullptr) {
        // Query does not know about this column. Just ignore it.
        continue;
      }
      switch (i->value.getTag()) {
      case JsonTag::JSON_STRING:
        column_info.data->emplace_back(i->value.toString());
        preprocessColumn(column_info.type, &column_info.data->back());
        break;
      case JsonTag::JSON_NULL:
        column_info.data->push_back(folly::none);
        break;
      default:
        ld_error("Expecting string or null value for column value, "
                 "but got %i",
                 i->value.getTag());
        return false;
      }
    }
    if (col != columns_present.size()) {
      ld_error("Invalid json table: header has %lu columns, but row %lu has "
               "%lu columns",
               columns_present.size(), row_idx, col);
      return false;
    }
    return true;
  };

  auto parse_rows = [&](const JsonValue& o) {
    if (o.getTag() != JsonTag::JSON_ARRAY) {
      ld_error("Expecting array for rows");
      return false;
    }
    size_t row_idx = 0;
    for (const auto& i : o) {
      if (!parse_row(row_idx++, i->value)) {
        return false;
      }
    }
    return true;
  };

  JsonNode* headers = nullptr;
  JsonNode* rows = nullptr;

  switch (value.getTag()) {
  case JsonTag::JSON_OBJECT:
    // extract "content"
    for (const auto& i : value) {
      if (i->key == std::string("type")) {
        hs_check(i->value.getTag() == JsonTag::JSON_STRING);
        hs_check(i->value.toString() == std::string("table"));
      } else if (i->key == std::string("content")) {
        value = i->value;
      }
    }
    for (const auto& i : value) {
      if (i->key == std::string("headers")) {
        headers = i;
      } else if (i->key == std::string("rows")) {
        rows = i;
      }
    }
    break;
  default:
    ld_info("Root of json tree is not an object.");
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  };

  if (!headers) {
    ld_error("Missing headers section");
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }
  if (!rows) {
    ld_error("Missing rows section");
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }

  if (!parse_headers(headers->value) || !parse_rows(rows->value)) {
    return PartialTableData{folly::none, false, "MALFORMED_RESPONSE"};
  }

  return PartialTableData{std::move(results), true, ""};
}

PartialTableData
AdminCommandTable::transformData(std::string response_from_node) const {
  switch (type_) {
  case Type::JSON_TABLE:
    return jsonToTableData(std::move(response_from_node));
  }

  // Should never be here, the compiler should complain if there is a missing
  // case in the above switch statement.  hs_check(false);
  return PartialTableData{folly::none, false, "UNEXPECTED"};
}

std::vector<ldquery::TableData> AdminCommandTable::transformDataParallel(
    std::vector<AdminCommandClient::Response>& responses) {
  std::vector<ldquery::TableData> outputs(responses.size());

  auto thread_func = [&](size_t from, size_t to) {
    for (size_t i = from; i < std::min(to, responses.size()); ++i) {
      if (responses[i].success) {
        PartialTableData partial_data =
            transformData(std::move(responses[i].response));
        if (partial_data.success) {
          outputs[i] = std::move(*(partial_data.data));
        } else {
          responses[i].success = false;
          responses[i].failure_reason = partial_data.failure_reason;
        }
      }
    }
  };

  const size_t num_threads = 32;
  const size_t num_per_thread = responses.size() / num_threads + 1;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads && i * num_per_thread < responses.size();
       i++) {
    const size_t from = i * num_per_thread;
    const size_t to = from + num_per_thread;
    threads.push_back(std::thread(thread_func, from, to));
  }

  for (int i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }

  return outputs;
}

std::vector<ServerNode>
AdminCommandTable::selectNodes(std::vector<ServerNode>& nodes,
                               ldquery::QueryContext& ctx) const {
  // Look for constraints on column 0 ("node_id").
  const int col_index = 0;
  auto it_c = ctx.constraints.find(col_index);
  std::unordered_set<int> used_constraints;
  std::vector<ServerNode> res;

  for (const auto& node : nodes) {
    bool skip = false;
    if (it_c != ctx.constraints.end() && allowServerSideFiltering()) {
      const ldquery::ConstraintList& constraints = it_c->second;
      for (int i = 0; i < constraints.constraints_.size(); ++i) {
        const ldquery::Constraint& c = constraints.constraints_[i];
        // NOTE: the node.id() returns an unsigned int32
        auto res = nodeMatchesConstraint(node.id(), c);
        if (res != MatchResult::UNUSED) {
          used_constraints.insert(i);
        }
        skip |= res == MatchResult::NO_MATCH;
      }
    }

    if (!skip) {
      // The node matches all used constraints.
      res.push_back(node);
    }
  }

  // Keep track of what constraints were used.
  for (int i : used_constraints) {
    ctx.used_constraints[col_index].add(
        ctx.constraints[col_index].constraints_[i]);
  }

  return res;
}

void AdminCommandTable::refillCache(ldquery::QueryContext& ctx) {
  auto client = ctx_->getClient();
  auto nodes_configuration = client->getNodesConfiguration();
  hs_check(nodes_configuration, "GetNodesConfiguration failed");

  // `selectNodes` may decide to select by `node_id`. In that case it will
  // mutate `ctx.used_constraints`.
  const auto selected_nodes = selectNodes(*nodes_configuration, ctx);

  auto used_constraints = ctx.used_constraints;

  std::string cmd;

  if (allowServerSideFiltering()) {
    // Find the admin command that needs to be sent to get the minimum amount of
    // data given the query constraints. This function populates
    // `ctx.used_constraints` which are the constraints  it could use to perform
    // server-side filtering.
    cmd = getCommandToSend(ctx);
    if (dataIsInCacheForUsedConstraints(ctx.used_constraints)) {
      ld_info("Using cached results for admin command '%s'",
              folly::rtrimWhitespace(cmd.c_str()).str().c_str());
      return;
    }
  }

  if (!allowServerSideFiltering() || num_fetches_ > MAX_FETCHES) {
    // We can be here for two reasons:
    //
    // 1/ server side filtering is enabled but we did too many fetches already,
    //    we decide it's best to just fetch everything from the server and index
    //    locally.
    // 2/ server side filtering is disabled.
    //
    // Call `getCommandToSend()` again without allowing any server-side
    // filtering. Reset `ctx.used_constraints` to the previous value before the
    // initial call to `getCommandToSend` (assuming 1/) and pass an empty
    // `constraints` map so that the next call to getCommandToSend() will not
    // leverage any constraints for server-side filtering;
    ctx.used_constraints = std::move(used_constraints);
    auto constraints = std::move(ctx.constraints);
    cmd = getCommandToSend(ctx);
    ctx.constraints = std::move(constraints);
    if (dataIsInCacheForUsedConstraints(ctx.used_constraints)) {
      ld_info("Using cached results for admin command '%s'",
              folly::rtrimWhitespace(cmd.c_str()).str().c_str());
      return;
    }
  }

  std::vector<AdminCommandClient::Request> requests;
  requests.reserve(selected_nodes.size());

  for (auto& node : selected_nodes) {
    auto addr = node.host() + ":" + std::to_string(node.port());
    requests.emplace_back(addr, cmd);
    ctx_->activeQueryMetadata.contacted_nodes++;
  }

  ld_info("Sending '%s' admin command to %lu nodes...",
          folly::rtrimWhitespace(cmd.c_str()).str().c_str(), requests.size());

  steady_clock::time_point tstart = steady_clock::now();
  auto responses = client->send(requests);
  hs_check(requests.size() == responses.size());
  steady_clock::time_point tend = steady_clock::now();
  double duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();

  size_t replies = 0;
  for (const auto& r : responses) {
    replies += r.success;
  }
  ld_info("Receiving data took %.1fs, %lu/%lu nodes replied", duration, replies,
          responses.size());
  tstart = tend;

  ld_info("Parsing json data...");
  std::vector<ldquery::TableData> results = transformDataParallel(responses);
  hs_check(results.size() == responses.size());
  for (int i = 0; i < results.size(); ++i) {
    if (!results[i].cols.empty()) {
      size_t rows = results[i].cols.begin()->second.size();
      std::string node_id_str = std::to_string(selected_nodes[i].id());
      results[i].cols["node_id"] = ldquery::Column(rows, node_id_str);
    }
  }

  for (int i = 0; i < requests.size(); i++) {
    if (!responses[i].success) {
      auto node_id = selected_nodes[i].id();
      ld_info("Failed request for N%d (%s): %s", node_id,
              requests[i].addr.c_str(), responses[i].failure_reason.c_str());
      ctx_->activeQueryMetadata.failures[node_id] = ldquery::FailedNodeDetails{
          requests[i].addr, responses[i].failure_reason};
    }
  }

  tend = steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info("Json parsing took %.1fs", duration);

  tstart = tend;
  Data data;
  ld_info("Aggregating data from %lu nodes...", responses.size());
  data.data =
      std::make_shared<ldquery::TableData>(aggregate(std::move(results)));

  tend = steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(tend - tstart)
          .count();
  ld_info("Aggregating %lu rows took %.1fs", data.data->numRows(), duration);

  admin_cmd_cache_[ctx.used_constraints] =
      std::make_unique<DataWithTTL>(DataWithTTL(std::move(data)));

  ++num_fetches_;
}

}}} // namespace hstream::client::query
