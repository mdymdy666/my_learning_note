/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#pragma once

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

namespace mini_redis
{

  inline std::string nowTime()
  {
    using namespace std::chrono;
    auto t = system_clock::to_time_t(system_clock::now());
    char buf[64];
    std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&t));
    return std::string(buf);
  }

#define MR_LOG(level, msg)                                                       \
  do                                                                             \
  {                                                                              \
    std::cerr << "[" << nowTime() << "] [" << level << "] " << msg << std::endl; \
  } while (0)

} // namespace mini_redis
