# 编译器设置
CXX = g++
CXXFLAGS = -Wall -Wextra -I.  # 开启警告，包含当前目录头文件
LDFLAGS = 

# 查找当前目录下所有.cpp文件
SRCS := $(wildcard *.cpp)
# 生成对应的可执行文件列表（去掉.cpp扩展名）
TARGETS := $(SRCS:.cpp=)

# 默认目标：编译所有可执行文件
all: $(TARGETS)

# 自动生成依赖文件（.d文件，记录头文件依赖）
DEPS := $(SRCS:.cpp=.d)
-include $(DEPS)
CXXFLAGS += -MMD -MP  # 自动生成依赖的编译选项

# 编译规则：将每个.cpp文件编译为同名可执行文件
%: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
	@echo "编译完成: $@"

# 清理目标：删除生成的可执行文件和依赖文件
clean:
	rm -f $(TARGETS) $(DEPS)
	@echo "已清理所有可执行文件和依赖文件"

# 伪目标：避免与同名文件冲突
.PHONY: all clean
