/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#include "mini_redis/server.hpp"

#include "mini_redis/resp.hpp"
#include "mini_redis/kv.hpp"
#include "mini_redis/config.hpp"
#include "mini_redis/log.hpp"
#include "mini_redis/aof.hpp"
#include "mini_redis/rdb.hpp"
#include "mini_redis/replica_client.hpp"
#include "mini_redis/state.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstring>
#include <cctype>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_redis
{

  // ServerConfig moved to config.hpp

  namespace
  {

    int set_nonblocking(int fd)
    {
      int flags = fcntl(fd, F_GETFL, 0);
      if (flags < 0)
        return -1;
      if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
      return 0;
    }

    int add_epoll(int epfd, int fd, uint32_t events)
    {
      epoll_event ev{};
      ev.events = events;
      ev.data.fd = fd;
      return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    }

    int mod_epoll(int epfd, int fd, uint32_t events)
    {
      epoll_event ev{};
      ev.events = events;
      ev.data.fd = fd;
      return epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }

    struct Conn
    {
      int fd = -1;
      std::string in = "";
      std::vector<std::string> out_chunks = {}; // 待发送块队列
      size_t out_iov_idx = 0;                   // 当前发送到第几个块
      size_t out_offset = 0;                    // 当前块内偏移
      RespParser parser = {};
      bool is_replica = false;
    };

  } // namespace

  Server::Server(const ServerConfig &config) : config_(config) {}
  Server::~Server()
  {
    if (listen_fd_ >= 0)
      close(listen_fd_);
    if (epoll_fd_ >= 0)
      close(epoll_fd_);
  }

  int Server::setupListen()
  {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
    {
      std::perror("socket");
      return -1;
    }

    int yes = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(config_.port));
    if (inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr) != 1)
    {
      MR_LOG("ERROR", "Invalid bind address: " << config_.bind_address);
      return -1;
    }

    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
      std::perror("bind");
      return -1;
    }
    if (set_nonblocking(listen_fd_) < 0)
    {
      std::perror("fcntl");
      return -1;
    }
    if (listen(listen_fd_, 512) < 0)
    {
      std::perror("listen");
      return -1;
    }
    return 0;
  }

  int Server::setupEpoll()
  {
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0)
    {
      std::perror("epoll_create1");
      return -1;
    }
    if (add_epoll(epoll_fd_, listen_fd_, EPOLLIN | EPOLLET) < 0)
    {
      std::perror("epoll_ctl add");
      return -1;
    }
    // setup periodic timer for active expire scan
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0)
    {
      std::perror("timerfd_create");
      return -1;
    }
    itimerspec its{};
    its.it_interval.tv_sec = 0;
    its.it_interval.tv_nsec = 200 * 1000 * 1000; // 200ms
    its.it_value = its.it_interval;
    if (timerfd_settime(timer_fd_, 0, &its, nullptr) < 0)
    {
      std::perror("timerfd_settime");
      return -1;
    }
    if (add_epoll(epoll_fd_, timer_fd_, EPOLLIN | EPOLLET) < 0)
    {
      std::perror("epoll_ctl add timer");
      return -1;
    }
    return 0;
  }

  KeyValueStore g_store;
  static AofLogger g_aof;
  static Rdb g_rdb;
  static std::vector<std::vector<std::string>> g_repl_queue;
  static inline bool has_pending(const Conn &c)
  {
    return c.out_iov_idx < c.out_chunks.size() || (c.out_iov_idx == c.out_chunks.size() && c.out_offset != 0);
  }

  // Try to flush pending output immediately without waiting for EPOLLOUT.
  static void try_flush_now(int fd, Conn &c, uint32_t &ev)
  {
    while (has_pending(c))
    {
      const size_t max_iov = 64;
      struct iovec iov[max_iov];
      int iovcnt = 0;
      size_t idx = c.out_iov_idx;
      size_t off = c.out_offset;
      while (idx < c.out_chunks.size() && iovcnt < (int)max_iov)
      {
        const std::string &s = c.out_chunks[idx];
        const char *base = s.data();
        size_t len = s.size();
        if (off >= len)
        {
          ++idx;
          off = 0;
          continue;
        }
        iov[iovcnt].iov_base = (void *)(base + off);
        iov[iovcnt].iov_len = len - off;
        ++iovcnt;
        ++idx;
        off = 0;
      }
      if (iovcnt == 0)
        break;
      ssize_t w = ::writev(fd, iov, iovcnt);
      if (w > 0)
      {
        size_t rem = (size_t)w;
        while (rem > 0 && c.out_iov_idx < c.out_chunks.size())
        {
          std::string &s = c.out_chunks[c.out_iov_idx];
          size_t avail = s.size() - c.out_offset;
          if (rem < avail)
          {
            c.out_offset += rem;
            rem = 0;
          }
          else
          {
            rem -= avail;
            c.out_offset = 0;
            ++c.out_iov_idx;
          }
        }
        if (c.out_iov_idx >= c.out_chunks.size())
        {
          c.out_chunks.clear();
          c.out_iov_idx = 0;
          c.out_offset = 0;
        }
      }
      else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
      {
        // need EPOLLOUT to continue later
        break;
      }
      else
      {
        std::perror("writev");
        ev |= EPOLLRDHUP;
        break;
      }
    }
  }

  static inline void enqueue_out(Conn &c, std::string s)
  {
    if (!s.empty())
      c.out_chunks.emplace_back(std::move(s));
  }

  static std::string g_repl_backlog;
  static const size_t kReplBacklogCap = 4 * 1024 * 1024; // 4MB
  static int64_t g_repl_offset = 0;                    // total bytes produced
  static int64_t g_backlog_start_offset = 0;           // offset of first byte in backlog buffer

  static void appendToBacklog(const std::string &data)
  {
    if (g_repl_backlog.size() + data.size() <= kReplBacklogCap)
    {
      g_repl_backlog.append(data);
    }
    else
    {
      size_t need = data.size();
      if (need >= kReplBacklogCap)
      {
        g_repl_backlog.assign(data.data() + (need - kReplBacklogCap), kReplBacklogCap);
      }
      else
      {
        size_t drop = (g_repl_backlog.size() + need) - kReplBacklogCap;
        g_repl_backlog.erase(0, drop);
        g_repl_backlog.append(data);
      }
    }
    // update start offset after appending
    g_backlog_start_offset = g_repl_offset - static_cast<int64_t>(g_repl_backlog.size());
  }

  static std::string handle_command(const RespValue &v, const std::string *raw)
  {
    if (v.type != RespType::kArray || v.array.empty())
      return respError("ERR protocol error");
    const auto &head = v.array[0];
    if (head.type != RespType::kBulkString && head.type != RespType::kSimpleString)
      return respError("ERR wrong type");
    std::string cmd;
    cmd.reserve(head.bulk.size());
    for (char c : head.bulk)
      cmd.push_back(static_cast<char>(::toupper(c)));

    if (cmd == "PING")
    {
      if (v.array.size() <= 1)
        return respSimpleString("PONG");
      if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString)
        return respBulk(v.array[1].bulk);
      return respError("ERR wrong number of arguments for 'PING'");
    }
    if (cmd == "ECHO")
    {
      if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString)
        return respBulk(v.array[1].bulk);
      return respError("ERR wrong number of arguments for 'ECHO'");
    }
    if (cmd == "SET")
    {
      if (v.array.size() < 3)
        return respError("ERR wrong number of arguments for 'SET'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
        return respError("ERR syntax");
      std::optional<int64_t> ttl_ms;
      // minimal options support: EX seconds or PX milliseconds
      size_t i = 3;
      while (i < v.array.size())
      {
        if (v.array[i].type != RespType::kBulkString)
          return respError("ERR syntax");
        std::string opt;
        opt.reserve(v.array[i].bulk.size());
        for (char ch : v.array[i].bulk) opt.push_back(static_cast<char>(::toupper(ch)));
        if (opt == "EX")
        {
          if (i + 1 >= v.array.size() || v.array[i + 1].type != RespType::kBulkString)
            return respError("ERR syntax");
          try {
            int64_t sec = std::stoll(v.array[i + 1].bulk);
            if (sec < 0) return respError("ERR invalid expire time in SET");
            ttl_ms = sec * 1000;
          } catch (...) { return respError("ERR value is not an integer or out of range"); }
          i += 2;
          continue;
        }
        else if (opt == "PX")
        {
          if (i + 1 >= v.array.size() || v.array[i + 1].type != RespType::kBulkString)
            return respError("ERR syntax");
          try {
            int64_t ms = std::stoll(v.array[i + 1].bulk);
            if (ms < 0) return respError("ERR invalid expire time in SET");
            ttl_ms = ms;
          } catch (...) { return respError("ERR value is not an integer or out of range"); }
          i += 2;
          continue;
        }
        else
        {
          // unsupported option for now
          return respError("ERR syntax");
        }
      }
      g_store.set(v.array[1].bulk, v.array[2].bulk, ttl_ms);
      if (raw)
        g_aof.appendRaw(*raw);
      else
      {
        std::vector<std::string> parts;
        parts.reserve(v.array.size());
        for (const auto &e : v.array)
          parts.push_back(e.bulk);
        g_aof.appendCommand(parts);
      }
      // replicate original args
      {
        std::vector<std::string> parts;
        parts.reserve(v.array.size());
        for (const auto &e : v.array)
          parts.push_back(e.bulk);
        g_repl_queue.push_back(std::move(parts));
      }
      return respSimpleString("OK");
    }
    if (cmd == "GET")
    {
      if (v.array.size() != 2)
        return respError("ERR wrong number of arguments for 'GET'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      auto val = g_store.get(v.array[1].bulk);
      if (!val.has_value())
        return respNullBulk();
      return respBulk(*val);
    }
    if (cmd == "KEYS")
    {
      // 允许 KEYS 或 KEYS <pattern>，未带 pattern 时等价 '*'
      std::string pattern = "*";
      if (v.array.size() == 2)
      {
        if (v.array[1].type == RespType::kBulkString || v.array[1].type == RespType::kSimpleString)
        {
          pattern = v.array[1].bulk;
        }
        else
        {
          return respError("ERR syntax");
        }
      }
      else if (v.array.size() != 1)
      {
        return respError("ERR wrong number of arguments for 'KEYS'");
      }
      // 仅支持 '*' 通配（返回所有 keys）。复杂模式可后续扩展。
      auto keys = g_store.listKeys();
      if (pattern != "*")
      {
        // 简易实现：不支持复杂 glob，直接返回空，或可实现简单前缀/后缀匹配
        // 这里先实现 '*'；其它模式后续扩展
        keys.clear();
      }
      std::string out = "*" + std::to_string(keys.size()) + "\r\n";
      for (const auto &k : keys)
        out += respBulk(k);
      return out;
    }
    if (cmd == "FLUSHALL")
    {
      if (v.array.size() != 1)
        return respError("ERR wrong number of arguments for 'FLUSHALL'");
      // 清空所有数据结构
      {
        // 直接使用快照删除，更安全的是在 KV 层提供 clear 接口
        auto s1 = g_store.snapshot();
        std::vector<std::string> keys;
        keys.reserve(s1.size());
        for (const auto &kvp : s1)
          keys.push_back(kvp.first);
        g_store.del(keys);
      }
      {
        auto s2 = g_store.snapshotHash();
        for (const auto &kvp : s2)
        {
          std::vector<std::string> flds;
          for (const auto &fv : kvp.second.fields)
            flds.push_back(fv.first);
          g_store.hdel(kvp.first, flds);
        }
      }
      {
        auto s3 = g_store.snapshotZSet();
        for (const auto &flat : s3)
        {
          std::vector<std::string> mems;
          mems.reserve(flat.items.size());
          for (const auto &it : flat.items)
            mems.push_back(it.second);
          g_store.zrem(flat.key, mems);
        }
      }
      // AOF 记录
      if (raw)
        g_aof.appendRaw(*raw);
      else
        g_aof.appendCommand({"FLUSHALL"});
      // 复制广播
      g_repl_queue.push_back({"FLUSHALL"});
      return respSimpleString("OK");
    }
    if (cmd == "DEL")
    {
      if (v.array.size() < 2)
        return respError("ERR wrong number of arguments for 'DEL'");
      std::vector<std::string> keys;
      keys.reserve(v.array.size() - 1);
      for (size_t i = 1; i < v.array.size(); ++i)
      {
        if (v.array[i].type != RespType::kBulkString)
          return respError("ERR syntax");
        keys.emplace_back(v.array[i].bulk);
      }
      int removed = g_store.del(keys);
      if (removed > 0)
      {
        std::vector<std::string> parts;
        parts.reserve(1 + keys.size());
        parts.emplace_back("DEL");
        for (auto &k : keys)
          parts.emplace_back(k);
        if (raw)
          g_aof.appendRaw(*raw);
        else
          g_aof.appendCommand(parts);
        g_repl_queue.push_back(parts);
      }
      return respInteger(removed);
    }
    if (cmd == "EXISTS")
    {
      if (v.array.size() != 2)
        return respError("ERR wrong number of arguments for 'EXISTS'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      bool ex = g_store.exists(v.array[1].bulk);
      return respInteger(ex ? 1 : 0);
    }
    if (cmd == "EXPIRE")
    {
      if (v.array.size() != 3)
        return respError("ERR wrong number of arguments for 'EXPIRE'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
        return respError("ERR syntax");
      try
      {
        int64_t seconds = std::stoll(v.array[2].bulk);
        bool ok = g_store.expire(v.array[1].bulk, seconds);
        if (ok)
        {
          if (raw)
            g_aof.appendRaw(*raw);
          else
            g_aof.appendCommand({"EXPIRE", v.array[1].bulk, std::to_string(seconds)});
        }
        if (ok)
          g_repl_queue.push_back({"EXPIRE", v.array[1].bulk, std::to_string(seconds)});
        return respInteger(ok ? 1 : 0);
      }
      catch (...)
      {
        return respError("ERR value is not an integer or out of range");
      }
    }
    if (cmd == "TTL")
    {
      if (v.array.size() != 2)
        return respError("ERR wrong number of arguments for 'TTL'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      int64_t t = g_store.ttl(v.array[1].bulk);
      return respInteger(t);
    }
    if (cmd == "HSET")
    {
      if (v.array.size() != 4)
        return respError("ERR wrong number of arguments for 'HSET'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString || v.array[3].type != RespType::kBulkString)
        return respError("ERR syntax");
      int created = g_store.hset(v.array[1].bulk, v.array[2].bulk, v.array[3].bulk);
      if (raw)
        g_aof.appendRaw(*raw);
      else
        g_aof.appendCommand({"HSET", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
      g_repl_queue.push_back({"HSET", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
      return respInteger(created);
    }
    if (cmd == "HGET")
    {
      if (v.array.size() != 3)
        return respError("ERR wrong number of arguments for 'HGET'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
        return respError("ERR syntax");
      auto val = g_store.hget(v.array[1].bulk, v.array[2].bulk);
      if (!val.has_value())
        return respNullBulk();
      return respBulk(*val);
    }
    if (cmd == "HDEL")
    {
      if (v.array.size() < 3)
        return respError("ERR wrong number of arguments for 'HDEL'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      std::vector<std::string> fields;
      for (size_t i = 2; i < v.array.size(); ++i)
      {
        if (v.array[i].type != RespType::kBulkString)
          return respError("ERR syntax");
        fields.emplace_back(v.array[i].bulk);
      }
      int removed = g_store.hdel(v.array[1].bulk, fields);
      if (removed > 0)
      {
        std::vector<std::string> parts;
        parts.reserve(2 + fields.size());
        parts.emplace_back("HDEL");
        parts.emplace_back(v.array[1].bulk);
        for (auto &f : fields)
          parts.emplace_back(f);
        if (raw)
          g_aof.appendRaw(*raw);
        else
          g_aof.appendCommand(parts);
        g_repl_queue.push_back(parts);
      }
      return respInteger(removed);
    }
    if (cmd == "HEXISTS")
    {
      if (v.array.size() != 3)
        return respError("ERR wrong number of arguments for 'HEXISTS'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
        return respError("ERR syntax");
      bool ex = g_store.hexists(v.array[1].bulk, v.array[2].bulk);
      return respInteger(ex ? 1 : 0);
    }
    if (cmd == "HGETALL")
    {
      if (v.array.size() != 2)
        return respError("ERR wrong number of arguments for 'HGETALL'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      auto flat = g_store.hgetallFlat(v.array[1].bulk);
      RespValue arr;
      arr.type = RespType::kArray;
      arr.array.reserve(flat.size());
      std::string out = "*" + std::to_string(flat.size()) + "\r\n";
      for (const auto &s : flat)
      {
        out += respBulk(s);
      }
      return out;
    }
    if (cmd == "HLEN")
    {
      if (v.array.size() != 2)
        return respError("ERR wrong number of arguments for 'HLEN'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      int n = g_store.hlen(v.array[1].bulk);
      return respInteger(n);
    }
    if (cmd == "ZADD")
    {
      if (v.array.size() != 4)
        return respError("ERR wrong number of arguments for 'ZADD'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString || v.array[3].type != RespType::kBulkString)
        return respError("ERR syntax");
      try
      {
        double sc = std::stod(v.array[2].bulk);
        int added = g_store.zadd(v.array[1].bulk, sc, v.array[3].bulk);
        if (raw)
          g_aof.appendRaw(*raw);
        else
          g_aof.appendCommand({"ZADD", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
        g_repl_queue.push_back({"ZADD", v.array[1].bulk, v.array[2].bulk, v.array[3].bulk});
        return respInteger(added);
      }
      catch (...)
      {
        return respError("ERR value is not a valid float");
      }
    }
    if (cmd == "ZREM")
    {
      if (v.array.size() < 3)
        return respError("ERR wrong number of arguments for 'ZREM'");
      if (v.array[1].type != RespType::kBulkString)
        return respError("ERR syntax");
      std::vector<std::string> members;
      for (size_t i = 2; i < v.array.size(); ++i)
      {
        if (v.array[i].type != RespType::kBulkString)
          return respError("ERR syntax");
        members.emplace_back(v.array[i].bulk);
      }
      int removed = g_store.zrem(v.array[1].bulk, members);
      if (removed > 0)
      {
        std::vector<std::string> parts;
        parts.reserve(2 + members.size());
        parts.emplace_back("ZREM");
        parts.emplace_back(v.array[1].bulk);
        for (auto &m : members)
          parts.emplace_back(m);
        if (raw)
          g_aof.appendRaw(*raw);
        else
          g_aof.appendCommand(parts);
        g_repl_queue.push_back(parts);
      }
      return respInteger(removed);
    }
    if (cmd == "ZRANGE")
    {
      if (v.array.size() != 4)
        return respError("ERR wrong number of arguments for 'ZRANGE'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString || v.array[3].type != RespType::kBulkString)
        return respError("ERR syntax");
      try
      {
        int64_t start = std::stoll(v.array[2].bulk);
        int64_t stop = std::stoll(v.array[3].bulk);
        auto members = g_store.zrange(v.array[1].bulk, start, stop);
        std::string out = "*" + std::to_string(members.size()) + "\r\n";
        for (const auto &m : members)
          out += respBulk(m);
        return out;
      }
      catch (...)
      {
        return respError("ERR value is not an integer or out of range");
      }
    }
    if (cmd == "ZSCORE")
    {
      if (v.array.size() != 3)
        return respError("ERR wrong number of arguments for 'ZSCORE'");
      if (v.array[1].type != RespType::kBulkString || v.array[2].type != RespType::kBulkString)
        return respError("ERR syntax");
      auto s = g_store.zscore(v.array[1].bulk, v.array[2].bulk);
      if (!s.has_value())
        return respNullBulk();
      return respBulk(std::to_string(*s));
    }
    if (cmd == "BGSAVE" || cmd == "SAVE")
    {
      if (v.array.size() != 1)
        return respError("ERR wrong number of arguments for 'BGSAVE'");
      std::string err;
      if (!g_rdb.save(g_store, err))
      {
        return respError(std::string("ERR rdb save failed: ") + err);
      }
      return respSimpleString("OK");
    }
    if (cmd == "BGREWRITEAOF")
    {
      if (v.array.size() != 1)
        return respError("ERR wrong number of arguments for 'BGREWRITEAOF'");
      std::string err;
      if (!g_aof.isEnabled())
        return respError("ERR AOF disabled");
      if (!g_aof.bgRewrite(g_store, err))
      {
        return respError(std::string("ERR ") + err);
      }
      return respSimpleString("OK");
    }
    if (cmd == "CONFIG")
    {
      if (v.array.size() < 2)
        return respError("ERR wrong number of arguments for 'CONFIG'");
      if (v.array[1].type != RespType::kBulkString && v.array[1].type != RespType::kSimpleString)
        return respError("ERR syntax");
      std::string sub;
      for (char c : v.array[1].bulk)
        sub.push_back(static_cast<char>(::toupper(c)));
      if (sub == "GET")
      {
        // 允许 CONFIG GET 与 CONFIG GET <pattern>（未提供时默认 "*")
        std::string pattern = "*";
        if (v.array.size() >= 3)
        {
          if (v.array[2].type != RespType::kBulkString && v.array[2].type != RespType::kSimpleString)
            return respError("ERR wrong number of arguments for 'CONFIG GET'");
          pattern = v.array[2].bulk;
        }
        else if (v.array.size() != 2)
        {
          return respError("ERR wrong number of arguments for 'CONFIG GET'");
        }
        auto match = [&](const std::string &k) -> bool {
          if (pattern == "*") return true;
          return pattern == k;
        };
        std::vector<std::pair<std::string, std::string>> kvs;
        // minimal set to satisfy tooling
        kvs.emplace_back("appendonly", g_aof.isEnabled() ? "yes" : "no");
        std::string appendfsync;
        switch (g_aof.mode())
        {
        case AofMode::kNo:
          appendfsync = "no";
          break;
        case AofMode::kEverySec:
          appendfsync = "everysec";
          break;
        case AofMode::kAlways:
          appendfsync = "always";
          break;
        }
        kvs.emplace_back("appendfsync", appendfsync);
        kvs.emplace_back("dir", "./data");
        kvs.emplace_back("dbfilename", "dump.rdb");
        kvs.emplace_back("save", "");
        kvs.emplace_back("timeout", "0");
        kvs.emplace_back("databases", "16");
        kvs.emplace_back("maxmemory", "0");
        std::string body;
        size_t elems = 0;
        if (pattern == "*") {
          for (auto &p : kvs) { body += respBulk(p.first); body += respBulk(p.second); elems += 2; }
        } else {
          for (auto &p : kvs) { if (match(p.first)) { body += respBulk(p.first); body += respBulk(p.second); elems += 2; } }
        }
        return "*" + std::to_string(elems) + "\r\n" + body;
      }
      else if (sub == "RESETSTAT")
      {
        if (v.array.size() != 2)
          return respError("ERR wrong number of arguments for 'CONFIG RESETSTAT'");
        return respSimpleString("OK");
      }
      else
      {
        return respError("ERR unsupported CONFIG subcommand");
      }
    }
    if (cmd == "INFO")
    {
      // INFO [section] -> ignore section for now
      std::string info;
      info.reserve(512);
      info += "# Server\r\nredis_version:0.1.0\r\nrole:master\r\n";
      info += "# Clients\r\nconnected_clients:0\r\n";
      info += "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\n";
      info += "# Persistence\r\naof_enabled:";
      info += (g_aof.isEnabled() ? "1" : "0");
      info += "\r\naof_rewrite_in_progress:0\r\nrdb_bgsave_in_progress:0\r\n";
      info += "# Replication\r\nconnected_slaves:0\r\nmaster_repl_offset:" + std::to_string(g_repl_offset) + "\r\n";
      return respBulk(info);
    }
    return respError("ERR unknown command");
  }

  int Server::loop()
  {
    std::unordered_map<int, Conn> conns;
    std::vector<epoll_event> events(128);
    while (true)
    {
      int n = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
      if (n < 0)
      {
        if (errno == EINTR)
          continue;
        std::perror("epoll_wait");
        return -1;
      }
      for (int i = 0; i < n; ++i)
      {
        int fd = events[i].data.fd;
        uint32_t ev = events[i].events;
        if (fd == listen_fd_)
        {
          while (true)
          {
            sockaddr_in cli{};
            socklen_t len = sizeof(cli);
            int cfd = accept(listen_fd_, reinterpret_cast<sockaddr *>(&cli), &len);
            if (cfd < 0)
            {
              if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
              std::perror("accept");
              break;
            }
            set_nonblocking(cfd);
            int one = 1;
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            add_epoll(epoll_fd_, cfd, EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLHUP);
            conns.emplace(cfd, Conn{cfd, std::string(), std::vector<std::string>{}, 0, 0, RespParser{}, false});
          }
          continue;
        }

        if (fd == timer_fd_)
        {
          while (true)
          {
            uint64_t ticks;
            ssize_t _r = ::read(timer_fd_, &ticks, sizeof(ticks));
            if (_r < 0)
            {
              if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
              else
                break;
            }
            if (_r == 0)
              break;
          }
          g_store.expireScanStep(64);
          continue;
        }

        auto it = conns.find(fd);
        if (it == conns.end())
          continue;
        Conn &c = it->second;

        // Immediate close only on EPOLLHUP or EPOLLERR; defer EPOLLRDHUP until after flushing replies
        if ((ev & EPOLLHUP) || (ev & EPOLLERR))
        {
          epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
          close(fd);
          conns.erase(it);
          continue;
        }

        if (ev & EPOLLIN)
        {
          char buf[4096];
          while (true)
          {
            ssize_t r = ::read(fd, buf, sizeof(buf));
            if (r > 0)
            {
              c.parser.append(std::string_view(buf, static_cast<size_t>(r)));
            }
            else if (r == 0)
            {
              ev |= EPOLLRDHUP;
              break;
            }
            else
            {
              if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
              std::perror("read");
              ev |= EPOLLRDHUP;
              break;
            }
          }
          while (true)
          {
            auto maybe = c.parser.tryParseOneWithRaw();
            if (!maybe.has_value())
              break;
            const RespValue &v = maybe->first;
            const std::string &raw = maybe->second;
            if (v.type == RespType::kError)
            {
              enqueue_out(c, respError("ERR protocol error"));
            }
            else
            {
              // Intercept SYNC: mark as replica and send RDB as RESP bulk
              if (v.type == RespType::kArray && !v.array.empty() &&
                  (v.array[0].type == RespType::kBulkString || v.array[0].type == RespType::kSimpleString))
              {
                std::string cmd;
                cmd.reserve(v.array[0].bulk.size());
                for (char ch : v.array[0].bulk)
                  cmd.push_back(static_cast<char>(::toupper(ch)));
                if (cmd == "PSYNC")
                {
                  // PSYNC <offset>
                  if (v.array.size() == 2 && v.array[1].type == RespType::kBulkString)
                  {
                    int64_t want = 0;
                    try
                    {
                      want = std::stoll(v.array[1].bulk);
                    }
                    catch (...)
                    {
                      want = -1;
                    }
                    // hit backlog?
                    if (want >= g_backlog_start_offset && want <= g_repl_offset)
                    {
                      size_t start = static_cast<size_t>(want - g_backlog_start_offset);
                      if (start < g_repl_backlog.size())
                      {
                        c.is_replica = true;
                        std::string off = "+OFFSET " + std::to_string(g_repl_offset) + "\r\n";
                        enqueue_out(c, off);
                        enqueue_out(c, g_repl_backlog.substr(start));
                        continue;
                      }
                    }
                  }
                  // fallback to full resync using SYNC path below
                }
                if (cmd == "SYNC")
                {
                  // produce RDB snapshot bytes
                  std::string err;
                  // Save to temp path and read back
                  RdbOptions tmp = config_.rdb;
                  if (!tmp.enabled)
                  {
                    tmp.enabled = true;
                  }
                  Rdb r(tmp);
                  if (!r.save(g_store, err))
                  {
                    enqueue_out(c, respError("ERR sync save failed"));
                  }
                  else
                  {
                    // read file
                    std::string path = r.path();
                    FILE *f = ::fopen(path.c_str(), "rb");
                    if (!f)
                    {
                      enqueue_out(c, respError("ERR open rdb"));
                    }
                    else
                    {
                      std::string content;
                      content.resize(0);
                      char rb[8192];
                      size_t m;
                      while ((m = fread(rb, 1, sizeof(rb), f)) > 0)
                        content.append(rb, m);
                      fclose(f);
                      enqueue_out(c, respBulk(content));
                      c.is_replica = true;
                      // 发送当前 offset（简单实现：用 RESP 简单字符串）
                      std::string off = "+OFFSET " + std::to_string(g_repl_offset) + "\r\n";
                      enqueue_out(c, std::move(off));
                    }
                  }
                  continue; // do not pass to normal handler
                }
              }
              enqueue_out(c, handle_command(v, &raw));
              // try immediate flush so pipe client can receive replies without waiting
              try_flush_now(fd, c, ev);
            }
          }
          // Broadcast any replication commands to replicas
          if (!g_repl_queue.empty())
          {
            for (auto &kv : conns)
            {
              Conn &rc = kv.second;
              if (!rc.is_replica)
                continue;
              for (const auto &parts : g_repl_queue)
              {
                std::string cmd = toRespArray(parts);
                int64_t next_off = g_repl_offset + static_cast<int64_t>(cmd.size());
                std::string off = "+OFFSET " + std::to_string(next_off) + "\r\n";
                appendToBacklog(off);
                appendToBacklog(cmd);
                g_repl_offset = next_off;
                enqueue_out(rc, std::move(off));
                enqueue_out(rc, std::move(cmd));
              }
              if (has_pending(rc))
              {
                mod_epoll(epoll_fd_, rc.fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
              }
            }
            g_repl_queue.clear();
          }
          if (has_pending(c))
          {
            mod_epoll(epoll_fd_, fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
          }
          // If peer half-closed and nothing pending, close now
          if ((ev & EPOLLRDHUP) && !has_pending(c))
          {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            close(fd);
            conns.erase(it);
            continue;
          }
        }

        if (ev & EPOLLOUT)
        {
          while (has_pending(c))
          {
            const size_t max_iov = 64;
            struct iovec iov[max_iov];
            int iovcnt = 0;
            size_t idx = c.out_iov_idx;
            size_t off = c.out_offset;
            while (idx < c.out_chunks.size() && iovcnt < (int)max_iov)
            {
              const std::string &s = c.out_chunks[idx];
              const char *base = s.data();
              size_t len = s.size();
              if (off >= len)
              {
                ++idx;
                off = 0;
                continue;
              }
              iov[iovcnt].iov_base = (void *)(base + off);
              iov[iovcnt].iov_len = len - off;
              ++iovcnt;
              ++idx;
              off = 0;
            }
            if (iovcnt == 0)
              break;
            ssize_t w = ::writev(fd, iov, iovcnt);
            if (w > 0)
            {
              size_t rem = (size_t)w;
              while (rem > 0 && c.out_iov_idx < c.out_chunks.size())
              {
                std::string &s = c.out_chunks[c.out_iov_idx];
                size_t avail = s.size() - c.out_offset;
                if (rem < avail)
                {
                  c.out_offset += rem;
                  rem = 0;
                }
                else
                {
                  rem -= avail;
                  c.out_offset = 0;
                  ++c.out_iov_idx;
                }
              }
              if (c.out_iov_idx >= c.out_chunks.size())
              {
                c.out_chunks.clear();
                c.out_iov_idx = 0;
                c.out_offset = 0;
              }
            }
            else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
              break;
            }
            else
            {
              std::perror("writev");
              ev |= EPOLLRDHUP;
              break;
            }
          }
          if (!has_pending(c))
          {
            mod_epoll(epoll_fd_, fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP);
            if (ev & EPOLLRDHUP)
            {
              epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
              close(fd);
              conns.erase(it);
              continue;
            }
          }
        }
      }
    }
  }

  int Server::run()
  {
    if (setupListen() < 0)
      return -1;
    if (setupEpoll() < 0)
      return -1;
    // init RDB then AOF and load
    if (config_.rdb.enabled)
    {
      g_rdb.setOptions(config_.rdb);
      std::string err;
      if (!g_rdb.load(g_store, err))
      {
        MR_LOG("ERROR", "RDB load failed: " << err);
        return -1;
      }
    }
    // init AOF and load
    if (config_.aof.enabled)
    {
      std::string err;
      if (!g_aof.init(config_.aof, err))
      {
        MR_LOG("ERROR", "AOF init failed: " << err);
        return -1;
      }
      if (!g_aof.load(g_store, err))
      {
        MR_LOG("ERROR", "AOF load failed: " << err);
        return -1;
      }
    }
    MR_LOG("INFO", "listening on " << config_.bind_address << ":" << config_.port);
    // start replica client if configured
    ReplicaClient repl(config_);
    repl.start();
    int rc = loop();
    repl.stop();
    return rc;
  }

} // namespace mini_redis
