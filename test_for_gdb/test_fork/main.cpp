#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>

int main() {
    int pipefd[2];
    pipe(pipefd);  // 创建管道

    pid_t pid = fork();  // 创建子进程
    if (pid == -1) {
        perror("fork failed");
        exit(1);
    }

    if (pid == 0) {  // 子进程：读管道
        close(pipefd[1]);  // 关闭写端
        char buf[100];
        read(pipefd[0], buf, sizeof(buf));
        printf("Child process received: %s\n", buf);
        close(pipefd[0]);
        exit(0);
    } else {  // 父进程：写管道
        close(pipefd[0]);  // 关闭读端
        char* msg = "Hello from parent!";
        write(pipefd[1], msg, sizeof(msg));
        close(pipefd[1]);
        
        wait(NULL);  // 等待子进程结束
        printf("Parent process finished\n");
    }
    return 0;
}


// (gdb) set detach-on-fork off  # 不分离子进程，所有进程都由gdb控制
// (gdb) set follow-fork-mode child  # 默认跟踪子进程（可改为parent）