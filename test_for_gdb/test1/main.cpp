#include <stdio.h>

int calculate_sum(int n) {
    int sum = 0;
    for (int i = 1; i < n; i++) {  // 逻辑错误：循环条件应为i <= n
        sum += i;
    }
    return sum;
}

int main() {
    int num = 5;
    int result = calculate_sum(num);
    printf("Sum from 1 to %d is: %d\n", num, result);
    return 0;
}