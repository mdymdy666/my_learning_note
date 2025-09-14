#!/bin/bash

# 定义所有需要创建的章节目录名（双引号包裹，空格分隔）
CHAPTERS=("第1章 TCP/IP协议族" "第2章 IP协议详解" "第3章 TCP协议详解" "第4章 TCP/IP通信案例：访问Internet上的Web服务器" "第5章 Linux网络编程基础API" "第6章 高级I/O函数" "第7章 Linux服务器程序规范" "第8章 高性能服务器程序框架" "第9章 I/O复用" "第10章 信号" "第11章 定时器" "第12章 高性能I/O框架库Libevent" "第13章 多进程编程" "第14章 多线程编程" "第15章 进程池和线程池" "第16章 服务器调制、调试和测试" "第17章 系统监测工具")

# 基础路径（默认在脚本所在目录，可根据需要修改，如 "/home/user/学习资料/"）
BASE_DIR="./"

# 遍历所有章节名并创建目录
for chapter in "${CHAPTERS[@]}"; do
    FULL_PATH="${BASE_DIR}/${chapter}"
    if [ ! -d "$FULL_PATH" ]; then
        mkdir -p "$FULL_PATH"
        echo "已创建目录：$FULL_PATH"
    else
        echo "目录已存在：$FULL_PATH"
    fi
done

echo "所有章节目录创建完成"