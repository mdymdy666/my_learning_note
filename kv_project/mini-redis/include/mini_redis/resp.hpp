/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mini_redis {

enum class RespType {
  kSimpleString,
  kError,
  kInteger,
  kBulkString,
  kArray,
  kNull
};

struct RespValue {
  RespType type = RespType::kNull;
  std::string bulk;               // for simple/bulk/error string or integer text
  std::vector<RespValue> array;   // for arrays
};

class RespParser {
 public:
  // Append newly read bytes into internal buffer
  void append(std::string_view data);

  // Try parse one full value. Return std::nullopt if incomplete; on error, returns value with type kError and bulk set to error msg
  std::optional<RespValue> tryParseOne();

  // Try parse one full value and also return the raw RESP bytes consumed
  std::optional<std::pair<RespValue, std::string>> tryParseOneWithRaw();

 private:
  // parse helpers
  bool parseLine(size_t& pos, std::string& out_line);
  bool parseInteger(size_t& pos, int64_t& out_value);
  bool parseBulkString(size_t& pos, RespValue& out);
  bool parseSimple(size_t& pos, RespType t, RespValue& out);
  bool parseArray(size_t& pos, RespValue& out);

 private:
  std::string buffer_;
};

// RESP serialization helpers
std::string respSimpleString(std::string_view s);
std::string respError(std::string_view s);
std::string respBulk(std::string_view s);
std::string respNullBulk();
std::string respInteger(int64_t v);

}  // namespace mini_redis


