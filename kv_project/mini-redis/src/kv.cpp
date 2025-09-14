/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#include "mini_redis/kv.hpp"

#include <chrono>
#include <algorithm>
#include <optional>
#include <iterator>
#include <cstdlib>

namespace mini_redis
{

  // ---------------- Skiplist implementation -----------------

  struct SkiplistNode
  {
    double score;
    std::string member;
    std::vector<SkiplistNode *> forward;
    SkiplistNode(int level, double sc, const std::string &mem)
        : score(sc), member(mem), forward(static_cast<size_t>(level), nullptr) {}
  };

  Skiplist::Skiplist() : head_(new SkiplistNode(kMaxLevel, 0.0, "")), level_(1), length_(0) {}
  Skiplist::~Skiplist()
  {
    SkiplistNode *cur = head_->forward[0];
    while (cur)
    {
      SkiplistNode *nxt = cur->forward[0];
      delete cur;
      cur = nxt;
    }
    delete head_;
  }

  int Skiplist::randomLevel()
  {
    int lvl = 1;
    while ((std::rand() & 0xFFFF) < static_cast<int>(kProbability * 0xFFFF) && lvl < kMaxLevel)
    {
      ++lvl;
    }
    return lvl;
  }

  static inline bool less_score_member(double a_sc, const std::string &a_m, double b_sc, const std::string &b_m)
  {
    if (a_sc != b_sc)
      return a_sc < b_sc;
    return a_m < b_m;
  }

  bool Skiplist::insert(double score, const std::string &member)
  {
    std::vector<SkiplistNode *> update(static_cast<size_t>(kMaxLevel));
    SkiplistNode *x = head_;
    for (int i = level_ - 1; i >= 0; --i)
    {
      while (x->forward[static_cast<size_t>(i)] &&
             less_score_member(x->forward[static_cast<size_t>(i)]->score,
                               x->forward[static_cast<size_t>(i)]->member,
                               score, member))
      {
        x = x->forward[static_cast<size_t>(i)];
      }
      update[static_cast<size_t>(i)] = x;
    }
    x = x->forward[0];
    if (x && x->score == score && x->member == member)
    {
      return false; // existed
    }
    int lvl = randomLevel();
    if (lvl > level_)
    {
      for (int i = level_; i < lvl; ++i)
        update[static_cast<size_t>(i)] = head_;
      level_ = lvl;
    }
    SkiplistNode *nx = new SkiplistNode(lvl, score, member);
    for (int i = 0; i < lvl; ++i)
    {
      nx->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
      update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = nx;
    }
    ++length_;
    return true;
  }

  bool Skiplist::erase(double score, const std::string &member)
  {
    std::vector<SkiplistNode *> update(static_cast<size_t>(kMaxLevel));
    SkiplistNode *x = head_;
    for (int i = level_ - 1; i >= 0; --i)
    {
      while (x->forward[static_cast<size_t>(i)] &&
             less_score_member(x->forward[static_cast<size_t>(i)]->score,
                               x->forward[static_cast<size_t>(i)]->member,
                               score, member))
      {
        x = x->forward[static_cast<size_t>(i)];
      }
      update[static_cast<size_t>(i)] = x;
    }
    x = x->forward[0];
    if (!x || x->score != score || x->member != member)
      return false;
    for (int i = 0; i < level_; ++i)
    {
      if (update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] == x)
      {
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = x->forward[static_cast<size_t>(i)];
      }
    }
    delete x;
    while (level_ > 1 && head_->forward[static_cast<size_t>(level_ - 1)] == nullptr)
    {
      --level_;
    }
    --length_;
    return true;
  }

  void Skiplist::rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const
  {
    if (length_ == 0)
      return;
    int64_t n = static_cast<int64_t>(length_);
    auto norm = [&](int64_t idx)
    {
      if (idx < 0)
        idx = n + idx;
      if (idx < 0)
        idx = 0;
      if (idx >= n)
        idx = n - 1;
      return idx;
    };
    int64_t s = norm(start), e = norm(stop);
    if (s > e)
      return;
    // walk from head level 0
    SkiplistNode *x = head_->forward[0];
    int64_t rank = 0;
    while (x && rank < s)
    {
      x = x->forward[0];
      ++rank;
    }
    while (x && rank <= e)
    {
      out.push_back(x->member);
      x = x->forward[0];
      ++rank;
    }
  }

  void Skiplist::toVector(std::vector<std::pair<double, std::string>> &out) const
  {
    out.clear();
    out.reserve(length_);
    SkiplistNode *x = head_->forward[0];
    while (x)
    {
      out.emplace_back(x->score, x->member);
      x = x->forward[0];
    }
  }

  // ---------------- KeyValueStore implementation -----------------

  int64_t KeyValueStore::nowMs()
  {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
  }

  bool KeyValueStore::isExpired(const ValueRecord &r, int64_t now_ms)
  {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
  }

  bool KeyValueStore::isExpired(const HashRecord &r, int64_t now_ms)
  {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
  }

  bool KeyValueStore::isExpired(const ZSetRecord &r, int64_t now_ms)
  {
    return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
  }

  void KeyValueStore::cleanupIfExpired(const std::string &key, int64_t now_ms)
  {
    auto it = map_.find(key);
    if (it == map_.end())
      return;
    if (isExpired(it->second, now_ms))
    {
      map_.erase(it);
      expire_index_.erase(key);
    }
  }

  void KeyValueStore::cleanupIfExpiredHash(const std::string &key, int64_t now_ms)
  {
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return;
    if (isExpired(it->second, now_ms))
    {
      hmap_.erase(it);
      expire_index_.erase(key);
    }
  }

  void KeyValueStore::cleanupIfExpiredZSet(const std::string &key, int64_t now_ms)
  {
    auto it = zmap_.find(key);
    if (it == zmap_.end())
      return;
    if (isExpired(it->second, now_ms))
    {
      zmap_.erase(it);
      expire_index_.erase(key);
    }
  }

  bool KeyValueStore::set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t expire_at = -1;
    if (ttl_ms.has_value())
    {
      expire_at = nowMs() + *ttl_ms;
    }
    map_[key] = ValueRecord{value, expire_at};
    if (expire_at >= 0) expire_index_[key] = expire_at; else expire_index_.erase(key);
    return true;
  }

  bool KeyValueStore::setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms)
  {
    std::lock_guard<std::mutex> lk(mu_);
    map_[key] = ValueRecord{value, expire_at_ms};
    if (expire_at_ms >= 0)
    {
      expire_index_[key] = expire_at_ms;
    }
    return true;
  }

  std::optional<std::string> KeyValueStore::get(const std::string &key)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
      return std::nullopt;
    return it->second.value;
  }

  bool KeyValueStore::exists(const std::string &key)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    return map_.find(key) != map_.end() || hmap_.find(key) != hmap_.end() || zmap_.find(key) != zmap_.end();
  }

  int KeyValueStore::del(const std::vector<std::string> &keys)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int removed = 0;
    int64_t now = nowMs();
    for (const auto &k : keys)
    {
      cleanupIfExpired(k, now);
      auto it = map_.find(k);
      if (it != map_.end())
      {
        map_.erase(it);
        expire_index_.erase(k);
        ++removed;
      }
    }
    return removed;
  }

  bool KeyValueStore::expire(const std::string &key, int64_t ttl_seconds)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
      return false;
    if (ttl_seconds < 0)
    {
      it->second.expire_at_ms = -1;
      expire_index_.erase(key);
      return true;
    }
    it->second.expire_at_ms = now + ttl_seconds * 1000;
    expire_index_[key] = it->second.expire_at_ms;
    return true;
  }

  int64_t KeyValueStore::ttl(const std::string &key)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpired(key, now);
    auto it = map_.find(key);
    if (it == map_.end())
      return -2; // key does not exist
    if (it->second.expire_at_ms < 0)
      return -1; // no expire
    int64_t ms_left = it->second.expire_at_ms - now;
    if (ms_left <= 0)
      return -2;
    return ms_left / 1000; // seconds (floor)
  }

  int KeyValueStore::expireScanStep(int max_steps)
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (max_steps <= 0 || expire_index_.empty()) return 0;
    int removed = 0;
    int64_t now = nowMs();
    // random starting point
    auto it = expire_index_.begin();
    std::advance(it, static_cast<long>(std::rand() % expire_index_.size())) ;
    for (int i = 0; i < max_steps && !expire_index_.empty(); ++i)
    {
      if (it == expire_index_.end()) it = expire_index_.begin();
      const std::string key = it->first;
      int64_t when = it->second;
      if (when >= 0 && now >= when)
      {
        // remove from all maps
        map_.erase(key);
        hmap_.erase(key);
        zmap_.erase(key);
        it = expire_index_.erase(it);
        ++removed;
      }
      else
      {
        ++it;
      }
    }
    return removed;
  }

  std::vector<std::pair<std::string, ValueRecord>> KeyValueStore::snapshot() const
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string, ValueRecord>> out;
    out.reserve(map_.size());
    for (const auto &kv : map_)
    {
      out.emplace_back(kv.first, kv.second);
    }
    return out;
  }

  std::vector<std::pair<std::string, HashRecord>> KeyValueStore::snapshotHash() const
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string, HashRecord>> out;
    out.reserve(hmap_.size());
    for (const auto &kv : hmap_)
      out.emplace_back(kv.first, kv.second);
    return out;
  }

  std::vector<KeyValueStore::ZSetFlat> KeyValueStore::snapshotZSet() const
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<ZSetFlat> out;
    out.reserve(zmap_.size());
    for (const auto &kv : zmap_)
    {
      ZSetFlat flat;
      flat.key = kv.first;
      flat.expire_at_ms = kv.second.expire_at_ms;
      if (!kv.second.use_skiplist)
        flat.items = kv.second.items;
      else
        kv.second.sl->toVector(flat.items);
      out.emplace_back(std::move(flat));
    }
    return out;
  }

  std::vector<std::string> KeyValueStore::listKeys() const
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::string> out;
    out.reserve(map_.size() + hmap_.size() + zmap_.size());
    for (const auto &kv : map_)
      out.push_back(kv.first);
    for (const auto &kv : hmap_)
      out.push_back(kv.first);
    for (const auto &kv : zmap_)
      out.push_back(kv.first);
    // 去重
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
  }

  int KeyValueStore::hset(const std::string &key, const std::string &field, const std::string &value)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto &rec = hmap_[key];
    auto it = rec.fields.find(field);
    if (it == rec.fields.end())
    {
      rec.fields[field] = value;
      return 1;
    }
    it->second = value;
    return 0;
  }

  std::optional<std::string> KeyValueStore::hget(const std::string &key, const std::string &field)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return std::nullopt;
    auto itf = it->second.fields.find(field);
    if (itf == it->second.fields.end())
      return std::nullopt;
    return itf->second;
  }

  int KeyValueStore::hdel(const std::string &key, const std::vector<std::string> &fields)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return 0;
    int removed = 0;
    for (const auto &f : fields)
    {
      auto itf = it->second.fields.find(f);
      if (itf != it->second.fields.end())
      {
        it->second.fields.erase(itf);
        ++removed;
      }
    }
    if (it->second.fields.empty())
    {
      hmap_.erase(it);
    }
    return removed;
  }

  bool KeyValueStore::hexists(const std::string &key, const std::string &field)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return false;
    return it->second.fields.find(field) != it->second.fields.end();
  }

  std::vector<std::string> KeyValueStore::hgetallFlat(const std::string &key)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    std::vector<std::string> out;
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return out;
    out.reserve(it->second.fields.size() * 2);
    for (const auto &kv : it->second.fields)
    {
      out.push_back(kv.first);
      out.push_back(kv.second);
    }
    return out;
  }

  int KeyValueStore::hlen(const std::string &key)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredHash(key, now);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return 0;
    return static_cast<int>(it->second.fields.size());
  }

  bool KeyValueStore::setHashExpireAtMs(const std::string &key, int64_t expire_at_ms)
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = hmap_.find(key);
    if (it == hmap_.end())
      return false;
    it->second.expire_at_ms = expire_at_ms;
    if (expire_at_ms >= 0)
      expire_index_[key] = expire_at_ms;
    else
      expire_index_.erase(key);
    return true;
  }

  int KeyValueStore::zadd(const std::string &key, double score, const std::string &member)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto &rec = zmap_[key];
    auto mit = rec.member_to_score.find(member);
    if (mit == rec.member_to_score.end())
    {
      if (!rec.use_skiplist)
      {
        auto &vec = rec.items;
        auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member),
                                   [](const auto &a, const auto &b)
                                   {
                                     if (a.first != b.first)
                                       return a.first < b.first;
                                     return a.second < b.second;
                                   });
        vec.insert(it, std::make_pair(score, member));
        if (vec.size() > kZsetVectorThreshold)
        {
          rec.use_skiplist = true;
          rec.sl = std::make_unique<Skiplist>();
          for (const auto &pr : vec)
            rec.sl->insert(pr.first, pr.second);
          std::vector<std::pair<double, std::string>>().swap(rec.items);
        }
      }
      else
      {
        rec.sl->insert(score, member);
      }
      rec.member_to_score.emplace(member, score);
      return 1;
    }
    else
    {
      double old = mit->second;
      if (old == score)
        return 0;
      if (!rec.use_skiplist)
      {
        auto &vec = rec.items;
        for (auto vit = vec.begin(); vit != vec.end(); ++vit)
        {
          if (vit->first == old && vit->second == member)
          {
            vec.erase(vit);
            break;
          }
        }
        auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member),
                                   [](const auto &a, const auto &b)
                                   {
                                     if (a.first != b.first)
                                       return a.first < b.first;
                                     return a.second < b.second;
                                   });
        vec.insert(it, std::make_pair(score, member));
        if (vec.size() > kZsetVectorThreshold)
        {
          rec.use_skiplist = true;
          rec.sl = std::make_unique<Skiplist>();
          for (const auto &pr : vec)
            rec.sl->insert(pr.first, pr.second);
          std::vector<std::pair<double, std::string>>().swap(rec.items);
        }
      }
      else
      {
        rec.sl->erase(old, member);
        rec.sl->insert(score, member);
      }
      mit->second = score;
      return 0;
    }
  }

  int KeyValueStore::zrem(const std::string &key, const std::vector<std::string> &members)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
      return 0;
    int removed = 0;
    for (const auto &m : members)
    {
      auto mit = it->second.member_to_score.find(m);
      if (mit == it->second.member_to_score.end())
        continue;
      double sc = mit->second;
      it->second.member_to_score.erase(mit);
      if (!it->second.use_skiplist)
      {
        auto &vec = it->second.items;
        for (auto vit = vec.begin(); vit != vec.end(); ++vit)
        {
          if (vit->first == sc && vit->second == m)
          {
            vec.erase(vit);
            ++removed;
            break;
          }
        }
      }
      else
      {
        if (it->second.sl->erase(sc, m))
          ++removed;
      }
    }
    if (!it->second.use_skiplist)
    {
      if (it->second.items.empty())
        zmap_.erase(it);
    }
    else
    {
      if (it->second.sl->size() == 0)
        zmap_.erase(it);
    }
    return removed;
  }

  std::vector<std::string> KeyValueStore::zrange(const std::string &key, int64_t start, int64_t stop)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    std::vector<std::string> out;
    auto it = zmap_.find(key);
    if (it == zmap_.end())
      return out;
    if (!it->second.use_skiplist)
    {
      const auto &vec = it->second.items;
      int64_t n = static_cast<int64_t>(vec.size());
      if (n == 0)
        return out;
      auto norm = [&](int64_t idx)
      { if (idx < 0) idx = n + idx; if (idx < 0) idx = 0; if (idx >= n) idx = n - 1; return idx; };
      int64_t s = norm(start), e = norm(stop);
      if (s > e)
        return out;
      for (int64_t i = s; i <= e; ++i)
        out.push_back(vec[static_cast<size_t>(i)].second);
    }
    else
    {
      it->second.sl->rangeByRank(start, stop, out);
    }
    return out;
  }

  std::optional<double> KeyValueStore::zscore(const std::string &key, const std::string &member)
  {
    std::lock_guard<std::mutex> lk(mu_);
    int64_t now = nowMs();
    cleanupIfExpiredZSet(key, now);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
      return std::nullopt;
    auto mit = it->second.member_to_score.find(member);
    if (mit == it->second.member_to_score.end())
      return std::nullopt;
    return mit->second;
  }

  bool KeyValueStore::setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms)
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = zmap_.find(key);
    if (it == zmap_.end())
      return false;
    it->second.expire_at_ms = expire_at_ms;
    if (expire_at_ms >= 0)
      expire_index_[key] = expire_at_ms;
    else
      expire_index_.erase(key);
    return true;
  }

} // namespace mini_redis
