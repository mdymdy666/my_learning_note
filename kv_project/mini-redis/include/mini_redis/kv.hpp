/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>

namespace mini_redis
{

  struct ValueRecord
  {
    std::string value;
    int64_t expire_at_ms = -1; // -1 means no expiration
  };

  struct HashRecord
  {
    std::unordered_map<std::string, std::string> fields;
    int64_t expire_at_ms = -1;
  };

  struct SkiplistNode;

  struct Skiplist
  {
    Skiplist();
    ~Skiplist();
    bool insert(double score, const std::string &member);
    bool erase(double score, const std::string &member);
    void rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const;
    void toVector(std::vector<std::pair<double, std::string>> &out) const;
    size_t size() const { return length_; }

  private:
    int randomLevel();
    static constexpr int kMaxLevel = 32;
    static constexpr double kProbability = 0.25;
    SkiplistNode *head_;
    int level_;
    size_t length_;
  };

  struct ZSetRecord
  {
    // 小集合使用 vector；超过阈值转为 skiplist
    bool use_skiplist = false;
    std::vector<std::pair<double, std::string>> items; // 当 use_skiplist=false 使用
    std::unique_ptr<Skiplist> sl;                      // 当 use_skiplist=true 使用
    std::unordered_map<std::string, double> member_to_score;
    int64_t expire_at_ms = -1;
  };

  class KeyValueStore
  {
  public:
    bool set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms = std::nullopt);
    bool setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms);
    std::optional<std::string> get(const std::string &key);
    int del(const std::vector<std::string> &keys);
    bool exists(const std::string &key);
    bool expire(const std::string &key, int64_t ttl_seconds);
    int64_t ttl(const std::string &key);
    size_t size() const { return map_.size(); }
    int expireScanStep(int max_steps);
    std::vector<std::pair<std::string, ValueRecord>> snapshot() const;
    std::vector<std::pair<std::string, HashRecord>> snapshotHash() const;
    struct ZSetFlat
    {
      std::string key;
      std::vector<std::pair<double, std::string>> items;
      int64_t expire_at_ms;
    };
    std::vector<ZSetFlat> snapshotZSet() const;
    std::vector<std::string> listKeys() const; // union of string/hash/zset keys (unique)

    // Hash APIs
    // returns 1 if new field created, 0 if overwritten
    int hset(const std::string &key, const std::string &field, const std::string &value);
    std::optional<std::string> hget(const std::string &key, const std::string &field);
    int hdel(const std::string &key, const std::vector<std::string> &fields);
    bool hexists(const std::string &key, const std::string &field);
    // return flatten [field, value, field, value, ...]
    std::vector<std::string> hgetallFlat(const std::string &key);
    int hlen(const std::string &key);
    bool setHashExpireAtMs(const std::string &key, int64_t expire_at_ms);

    // ZSet APIs
    // returns number of new elements added
    int zadd(const std::string &key, double score, const std::string &member);
    // returns number of members removed
    int zrem(const std::string &key, const std::vector<std::string> &members);
    // return members between start and stop (inclusive), negative indexes allowed
    std::vector<std::string> zrange(const std::string &key, int64_t start, int64_t stop);
    std::optional<double> zscore(const std::string &key, const std::string &member);
    bool setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms);

  private:
    static int64_t nowMs();
    static bool isExpired(const ValueRecord &r, int64_t now_ms);
    static bool isExpired(const HashRecord &r, int64_t now_ms);
    static bool isExpired(const ZSetRecord &r, int64_t now_ms);
    void cleanupIfExpired(const std::string &key, int64_t now_ms);
    void cleanupIfExpiredHash(const std::string &key, int64_t now_ms);
    void cleanupIfExpiredZSet(const std::string &key, int64_t now_ms);
    static constexpr size_t kZsetVectorThreshold = 128;

  private:
    std::unordered_map<std::string, ValueRecord> map_;
    std::unordered_map<std::string, ValueRecord>::iterator scan_it_ = map_.end();
    std::unordered_map<std::string, HashRecord> hmap_;
    std::unordered_map<std::string, HashRecord>::iterator hscan_it_ = hmap_.end();
    std::unordered_map<std::string, ZSetRecord> zmap_;
    std::unordered_map<std::string, ZSetRecord>::iterator zscan_it_ = zmap_.end();
    // Unified expire index for active expiration sampling
    std::unordered_map<std::string, int64_t> expire_index_;
    mutable std::mutex mu_;
  };

} // namespace mini_redis
