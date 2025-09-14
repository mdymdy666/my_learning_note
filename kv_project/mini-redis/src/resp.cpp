/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#include "mini_redis/resp.hpp"

#include <charconv>

namespace mini_redis
{

  void RespParser::append(std::string_view data) { buffer_.append(data.data(), data.size()); }

  bool RespParser::parseLine(size_t &pos, std::string &out_line)
  {
    size_t end = buffer_.find("\r\n", pos);
    if (end == std::string::npos)
      return false;
    out_line.assign(buffer_.data() + pos, end - pos);
    pos = end + 2;
    return true;
  }

  bool RespParser::parseInteger(size_t &pos, int64_t &out_value)
  {
    std::string line;
    if (!parseLine(pos, line))
      return false;
    auto first = line.data();
    auto last = line.data() + line.size();
    int64_t v = 0;
    auto [ptr, ec] = std::from_chars(first, last, v);
    if (ec != std::errc() || ptr != last)
      return false;
    out_value = v;
    return true;
  }

  bool RespParser::parseSimple(size_t &pos, RespType t, RespValue &out)
  {
    std::string s;
    if (!parseLine(pos, s))
      return false;
    out.type = t;
    out.bulk = std::move(s);
    return true;
  }

  bool RespParser::parseBulkString(size_t &pos, RespValue &out)
  {
    int64_t len = 0;
    if (!parseInteger(pos, len))
      return false;
    if (len == -1)
    {
      out.type = RespType::kNull;
      return true;
    }
    if (len < 0)
      return false;
    if (buffer_.size() < pos + static_cast<size_t>(len) + 2)
      return false;
    out.type = RespType::kBulkString;
    out.bulk.assign(buffer_.data() + pos, static_cast<size_t>(len));
    pos += static_cast<size_t>(len);
    if (!(pos + 1 < buffer_.size() && buffer_[pos] == '\r' && buffer_[pos + 1] == '\n'))
      return false;
    pos += 2;
    return true;
  }

  bool RespParser::parseArray(size_t &pos, RespValue &out)
  {
    int64_t count = 0;
    if (!parseInteger(pos, count))
      return false;
    if (count == -1)
    {
      out.type = RespType::kNull;
      return true;
    }
    if (count < 0)
      return false;
    out.type = RespType::kArray;
    out.array.clear();
    out.array.reserve(static_cast<size_t>(count));
    for (int64_t i = 0; i < count; ++i)
    {
      if (pos >= buffer_.size())
        return false;
      char prefix = buffer_[pos++];
      RespValue elem;
      bool ok = false;
      switch (prefix)
      {
      case '+':
        ok = parseSimple(pos, RespType::kSimpleString, elem);
        break;
      case '-':
        ok = parseSimple(pos, RespType::kError, elem);
        break;
      case ':':
      {
        int64_t v = 0;
        ok = parseInteger(pos, v);
        if (ok)
        {
          elem.type = RespType::kInteger;
          elem.bulk = std::to_string(v);
        }
        break;
      }
      case '$':
        ok = parseBulkString(pos, elem);
        break;
      case '*':
        ok = parseArray(pos, elem);
        break;
      default:
        return false;
      }
      if (!ok)
        return false;
      out.array.emplace_back(std::move(elem));
    }
    return true;
  }

  std::optional<RespValue> RespParser::tryParseOne()
  {
    if (buffer_.empty())
      return std::nullopt;
    size_t pos = 0;
    char prefix = buffer_[pos++];
    RespValue out;
    bool ok = false;
    switch (prefix)
    {
    case '+':
      ok = parseSimple(pos, RespType::kSimpleString, out);
      break;
    case '-':
      ok = parseSimple(pos, RespType::kError, out);
      break;
    case ':':
    {
      int64_t v = 0;
      ok = parseInteger(pos, v);
      if (ok)
      {
        out.type = RespType::kInteger;
        out.bulk = std::to_string(v);
      }
      break;
    }
    case '$':
      ok = parseBulkString(pos, out);
      break;
    case '*':
      ok = parseArray(pos, out);
      break;
    default:
      return RespValue{RespType::kError, std::string("protocol error"), {}};
    }
    if (!ok)
      return std::nullopt;
    buffer_.erase(0, pos);
    return out;
  }

  std::optional<std::pair<RespValue, std::string>> RespParser::tryParseOneWithRaw()
  {
    if (buffer_.empty())
      return std::nullopt;
    size_t pos = 0;
    char prefix = buffer_[pos++];
    RespValue out;
    bool ok = false;
    switch (prefix)
    {
    case '+':
      ok = parseSimple(pos, RespType::kSimpleString, out);
      break;
    case '-':
      ok = parseSimple(pos, RespType::kError, out);
      break;
    case ':':
    {
      int64_t v = 0;
      ok = parseInteger(pos, v);
      if (ok)
      {
        out.type = RespType::kInteger;
        out.bulk = std::to_string(v);
      }
      break;
    }
    case '$':
      ok = parseBulkString(pos, out);
      break;
    case '*':
      ok = parseArray(pos, out);
      break;
    default:
      return std::make_optional(std::make_pair(RespValue{RespType::kError, std::string("protocol error"), {}}, std::string()));
    }
    if (!ok)
      return std::nullopt;
    std::string raw(buffer_.data(), pos);
    buffer_.erase(0, pos);
    return std::make_pair(std::move(out), std::move(raw));
  }

  std::string respSimpleString(std::string_view s) { return std::string("+") + std::string(s) + "\r\n"; }
  std::string respError(std::string_view s) { return std::string("-") + std::string(s) + "\r\n"; }
  std::string respBulk(std::string_view s)
  {
    return "$" + std::to_string(s.size()) + "\r\n" + std::string(s) + "\r\n";
  }
  std::string respNullBulk() { return "$-1\r\n"; }
  std::string respInteger(int64_t v) { return ":" + std::to_string(v) + "\r\n"; }

} // namespace mini_redis
