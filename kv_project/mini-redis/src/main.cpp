/**
 * 创建者：程序员老廖
 * 日期：2025年8月12日
 */

#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include "mini_redis/server.hpp"
#include "mini_redis/config.hpp"
#include "mini_redis/config_loader.hpp"
#include "mini_redis/aof.hpp"

namespace mini_redis
{

  static volatile std::sig_atomic_t g_should_stop = 0;

  void handle_signal(int signum)
  {
    (void)signum;
    g_should_stop = 1;
  }

  void print_usage(const char *argv0)
  {
    std::cout << "mini-redis usage:\n"
              << "  " << argv0 << " [--port <port>] [--bind <ip>] [--config <file>]" << std::endl;
  }

  bool parse_args(int argc, char **argv, ServerConfig &out_config)
  {
    for (int i = 1; i < argc; ++i)
    {
      const std::string arg = argv[i];
      if (arg == "--port" && i + 1 < argc)
      {
        out_config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
      }
      else if (arg == "--bind" && i + 1 < argc)
      {
        out_config.bind_address = argv[++i];
      }
      else if (arg == "--config" && i + 1 < argc)
      {
        std::string file = argv[++i];
        std::string err;
        if (!mini_redis::loadConfigFromFile(file, out_config, err))
        {
          std::cerr << err << std::endl;
          return false;
        }
      }
      else if (arg == "-h" || arg == "--help")
      {
        print_usage(argv[0]);
        return false;
      }
      else
      {
        std::cerr << "Unknown argument: " << arg << std::endl;
        print_usage(argv[0]);
        return false;
      }
    }
    return true;
  }

  int run_server(const ServerConfig &config)
  {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    mini_redis::Server srv(config);
    return srv.run();
  }

} // namespace mini_redis

int main(int argc, char **argv)
{
  mini_redis::ServerConfig config;
  if (!mini_redis::parse_args(argc, argv, config))
  {
    return 1;
  }
  return mini_redis::run_server(config);
}
