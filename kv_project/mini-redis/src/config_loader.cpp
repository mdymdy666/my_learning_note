/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#include "mini_redis/config_loader.hpp"

#include <cctype>
#include <fstream>
#include <sstream>

#include "mini_redis/config.hpp"

namespace mini_redis
{

  static std::string trim(const std::string &s)
  {
    size_t i = 0, j = s.size();
    while (i < j && std::isspace(static_cast<unsigned char>(s[i])))
      ++i;
    while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1])))
      --j;
    return s.substr(i, j - i);
  }

  bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err)
  {
    std::ifstream in(path);
    if (!in.good())
    {
      err = "open config failed: " + path;
      return false;
    }
    std::string line;
    int lineno = 0;
    while (std::getline(in, line))
    {
      ++lineno;
      std::string t = trim(line);
      if (t.empty() || t[0] == '#')
        continue;
      auto pos = t.find('=');
      if (pos == std::string::npos)
      {
        err = "invalid line " + std::to_string(lineno);
        return false;
      }
      std::string key = trim(t.substr(0, pos));
      std::string val = trim(t.substr(pos + 1));
      if (key == "port")
      {
        try
        {
          cfg.port = static_cast<uint16_t>(std::stoi(val));
        }
        catch (...)
        {
          err = "invalid port at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "bind_address")
      {
        cfg.bind_address = val;
      }
      else if (key == "aof.enabled")
      {
        cfg.aof.enabled = (val == "1" || val == "true" || val == "yes");
      }
      else if (key == "aof.mode")
      {
        if (val == "no")
          cfg.aof.mode = AofMode::kNo;
        else if (val == "everysec")
          cfg.aof.mode = AofMode::kEverySec;
        else if (val == "always")
          cfg.aof.mode = AofMode::kAlways;
        else
        {
          err = "invalid aof.mode at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.dir")
      {
        cfg.aof.dir = val;
      }
      else if (key == "aof.filename")
      {
        cfg.aof.filename = val;
      }
      else if (key == "aof.batch_bytes")
      {
        try
        {
          cfg.aof.batch_bytes = static_cast<size_t>(std::stoull(val));
        }
        catch (...)
        {
          err = "invalid aof.batch_bytes at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.batch_wait_us")
      {
        try
        {
          cfg.aof.batch_wait_us = std::stoi(val);
        }
        catch (...)
        {
          err = "invalid aof.batch_wait_us at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.prealloc_bytes")
      {
        try
        {
          cfg.aof.prealloc_bytes = static_cast<size_t>(std::stoull(val));
        }
        catch (...)
        {
          err = "invalid aof.prealloc_bytes at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.sync_interval_ms")
      {
        try
        {
          cfg.aof.sync_interval_ms = std::stoi(val);
        }
        catch (...)
        {
          err = "invalid aof.sync_interval_ms at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.use_sync_file_range")
      {
        cfg.aof.use_sync_file_range = (val == "1" || val == "true" || val == "yes");
      }
      else if (key == "aof.sfr_min_bytes")
      {
        try
        {
          cfg.aof.sfr_min_bytes = static_cast<size_t>(std::stoull(val));
        }
        catch (...)
        {
          err = "invalid aof.sfr_min_bytes at line " + std::to_string(lineno);
          return false;
        }
      }
      else if (key == "aof.fadvise_dontneed_after_sync")
      {
        cfg.aof.fadvise_dontneed_after_sync = (val == "1" || val == "true" || val == "yes");
      }
      else if (key == "rdb.enabled")
      {
        cfg.rdb.enabled = (val == "1" || val == "true" || val == "yes");
      }
      else if (key == "rdb.dir")
      {
        cfg.rdb.dir = val;
      }
      else if (key == "rdb.filename")
      {
        cfg.rdb.filename = val;
      }
      else if (key == "replica.enabled")
      {
        cfg.replica.enabled = (val == "1" || val == "true" || val == "yes");
      }
      else if (key == "replica.master_host")
      {
        cfg.replica.master_host = val;
      }
      else if (key == "replica.master_port")
      {
        try
        {
          cfg.replica.master_port = static_cast<uint16_t>(std::stoi(val));
        }
        catch (...)
        {
          err = "invalid replica.master_port at line " + std::to_string(lineno);
          return false;
        }
      }
      else
      {
        // ignore unknown keys for forward compatibility
      }
    }
    return true;
  }

} // namespace mini_redis
