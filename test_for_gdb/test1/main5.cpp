#include <stdio.h>
#include <stdlib.h>

void risky_operation(int *ptr) {
    *ptr = 100;  // 如果ptr为NULL，会导致段错误
}

int main() {
    int *data = NULL;
    risky_operation(data);  // 传入NULL，触发崩溃
    return 0;
}