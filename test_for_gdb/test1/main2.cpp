#include <stdio.h>

void print_array(int* arr, int size) {
    for (int i = 0; i <= size+10; i++) {  // 错误：i <= size会访问arr[size]（越界）
        printf("%d ", arr[i]);
    }
    printf("\n");
}

int main() {
    int numbers[] = {1, 2, 3, 4, 5};
    int len = 5;
    print_array(numbers, len);  // 数组大小为5，索引0~4
    return 0;
}