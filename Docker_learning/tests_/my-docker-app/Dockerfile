# 1. 选择基础镜像（用轻量的Node.js镜像，alpine版本体积小）
FROM node:lts-alpine

# 2. 设置工作目录（容器内的目录，后续命令在此执行）
WORKDIR /app

# 3. 复制应用文件到容器的工作目录（.表示当前目录，复制到/app）
COPY app.js .

# 4. 定义容器启动时执行的命令（运行app.js）
CMD ["node", "app.js"]