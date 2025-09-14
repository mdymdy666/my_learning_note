#include <stdio.h>

int find_number() {
    int num = 1;
    while (1) {  // 死循环：条件判断错误
        if (num % 3 == 0 || num % 5 == 0) {  // 应为&&（同时满足）
            return num;
        }
        num++;
    }
}

int main() {
    int result = find_number();
    printf("First number divisible by 3 and 5: %d\n", result);
    return 0;
}