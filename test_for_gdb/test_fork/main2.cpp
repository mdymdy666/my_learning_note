#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>

int main() {
    int pipefd[2];
    if (pipe(pipefd) == -1) {  // 创建管道
        perror("pipe failed");
        exit(1);
    }

    pid_t pid = fork();  // 1. 创建子进程
    if (pid == -1) {
        perror("fork failed");
        exit(1);
    }

    if (pid == 0) {  // 子进程：读数据
        close(pipefd[1]);  // 关闭写端（必须关闭未使用的端）
        char buf[1024] = {0};
        ssize_t n = read(pipefd[0], buf, sizeof(buf)-1);  // 2. 读管道
        if (n == -1) {
            perror("read failed");
            exit(1);
        }
        printf("Child received: %s (size: %ld)\n", buf, n);
        close(pipefd[0]);
        exit(0);
    } else {  // 父进程：写数据
        close(pipefd[0]);  // 关闭读端
        const char* msg = "Hello from parent! This is a test message.";
        ssize_t n = write(pipefd[1], msg, strlen(msg));  // 3. 写管道
        if (n == -1) {
            perror("write failed");
            exit(1);
        }
        printf("Parent wrote %ld bytes\n", n);
        close(pipefd[1]);
        waitpid(pid, NULL, 0);  // 等待子进程结束
        printf("Parent exit\n");
    }
    return 0;
}