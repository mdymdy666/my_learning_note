#include <stdio.h>

void process_data(int *count) {
    // 模拟复杂逻辑：在某些条件下错误修改count
    if (*count > 5) {
        *count = 0;  // 错误操作：此处不应重置
    }
    // 其他处理...
}

int main() {
    int counter = 3;
    printf("Initial value: %d\n", counter);
    
    for (int i = 0; i < 10; i++) {
        counter += i;
        process_data(&counter);
        printf("After iteration %d: %d\n", i, counter);
    }
    return 0;
}