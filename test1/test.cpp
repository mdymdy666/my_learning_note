#include <iostream>
#include <vector>
#include <queue>
#include <climits>
#include <algorithm>
using namespace std;

// 图的边结构
struct Edge {
    int to;     // 目标节点
    int weight; // 边的权重
    
    Edge(int t, int w) : to(t), weight(w) {}
};

// 1. 迪杰斯特拉算法 (Dijkstra's Algorithm)
// 适用条件：边权非负的图，单源最短路径
vector<int> dijkstra(const vector<vector<Edge>>& graph, int start) {
    int n = graph.size();
    vector<int> dist(n, INT_MAX);  // 距离数组，记录到各点的最短距离
    vector<bool> visited(n, false); // 标记是否已确定最短路径
    
    dist[start] = 0;  // 起点到自身的距离为0
    
    for (int i = 0; i < n - 1; ++i) {
        // 找到未访问节点中距离最小的节点
        int u = -1;
        for (int v = 0; v < n; ++v) {
            if (!visited[v] && (u == -1 || dist[v] < dist[u])) {
                u = v;
            }
        }
        
        if (u == -1 || dist[u] == INT_MAX) break; // 无法到达更多节点
        
        visited[u] = true;
        
        // 松弛操作
        for (const Edge& e : graph[u]) {
            int v = e.to;
            int weight = e.weight;
            if (!visited[v] && dist[u] != INT_MAX && dist[u] + weight < dist[v]) {
                dist[v] = dist[u] + weight;
            }
        }
    }
    
    return dist;
}

// 2. 贝尔曼-福特算法 (Bellman-Ford Algorithm)
// 适用条件：可以处理负权边，单源最短路径，能检测负权环
bool bellmanFord(const vector<vector<Edge>>& graph, int start, vector<int>& dist) {
    int n = graph.size();
    dist.assign(n, INT_MAX);
    dist[start] = 0;
    
    // 进行n-1次松弛操作
    for (int i = 0; i < n - 1; ++i) {
        bool updated = false;
        for (int u = 0; u < n; ++u) {
            if (dist[u] == INT_MAX) continue;
            
            for (const Edge& e : graph[u]) {
                int v = e.to;
                int weight = e.weight;
                if (dist[v] > dist[u] + weight) {
                    dist[v] = dist[u] + weight;
                    updated = true;
                }
            }
        }
        if (!updated) break; // 没有更新，提前退出
    }
    
    // 检测负权环
    for (int u = 0; u < n; ++u) {
        if (dist[u] == INT_MAX) continue;
        
        for (const Edge& e : graph[u]) {
            int v = e.to;
            int weight = e.weight;
            if (dist[v] > dist[u] + weight) {
                return false; // 存在负权环
            }
        }
    }
    
    return true; // 没有负权环
}

// 3. 弗洛伊德算法 (Floyd-Warshall Algorithm)
// 适用条件：全源最短路径，可以处理负权边，但不能有负权环
vector<vector<int>> floydWarshall(const vector<vector<Edge>>& graph) {
    int n = graph.size();
    vector<vector<int>> dist(n, vector<int>(n, INT_MAX));
    
    // 初始化距离矩阵
    for (int i = 0; i < n; ++i) {
        dist[i][i] = 0; // 自身到自身距离为0
        for (const Edge& e : graph[i]) {
            dist[i][e.to] = e.weight;
        }
    }
    
    // 动态规划更新距离
    for (int k = 0; k < n; ++k) {          // 中间节点
        for (int i = 0; i < n; ++i) {      // 起点
            for (int j = 0; j < n; ++j) {  // 终点
                if (dist[i][k] != INT_MAX && dist[k][j] != INT_MAX) {
                    dist[i][j] = min(dist[i][j], dist[i][k] + dist[k][j]);
                }
            }
        }
    }
    
    return dist;
}

// 4. SPFA算法 (Shortest Path Faster Algorithm)
// 适用条件：可以处理负权边，单源最短路径，是Bellman-Ford的队列优化版本
bool spfa(const vector<vector<Edge>>& graph, int start, vector<int>& dist) {
    int n = graph.size();
    dist.assign(n, INT_MAX);
    vector<int> inQueue(n, 0); // 记录节点入队次数
    vector<bool> isInQueue(n, false); // 标记节点是否在队列中
    queue<int> q;
    
    dist[start] = 0;
    q.push(start);
    isInQueue[start] = true;
    inQueue[start]++;
    
    while (!q.empty()) {
        int u = q.front();
        q.pop();
        isInQueue[u] = false;
        
        for (const Edge& e : graph[u]) {
            int v = e.to;
            int weight = e.weight;
            
            if (dist[u] != INT_MAX && dist[v] > dist[u] + weight) {
                dist[v] = dist[u] + weight;
                
                if (!isInQueue[v]) {
                    q.push(v);
                    isInQueue[v] = true;
                    inQueue[v]++;
                    
                    // 如果一个节点入队次数超过n-1，说明存在负权环
                    if (inQueue[v] >= n) {
                        return false;
                    }
                }
            }
        }
    }
    
    return true;
}

// 打印最短路径结果
void printResult(const vector<int>& dist, int start) {
    int n = dist.size();
    cout << "从节点 " << start << " 出发的最短路径:" << endl;
    for (int i = 0; i < n; ++i) {
        if (dist[i] == INT_MAX) {
            cout << "到节点 " << i << ": 不可达" << endl;
        } else {
            cout << "到节点 " << i << ": " << dist[i] << endl;
        }
    }
    cout << endl;
}

// 打印全源最短路径结果
void printAllPairsResult(const vector<vector<int>>& dist) {
    int n = dist.size();
    cout << "全源最短路径矩阵:" << endl;
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            if (dist[i][j] == INT_MAX) {
                cout << "∞ ";
            } else {
                cout << dist[i][j] << "  ";
            }
        }
        cout << endl;
    }
    cout << endl;
}

int main() {
    // 创建一个示例图
    // 0 -> 1 (2), 0 -> 2 (4)
    // 1 -> 2 (1), 1 -> 3 (7)
    // 2 -> 3 (3)
    // 3 -> 4 (1)
    int n = 5;
    vector<vector<Edge>> graph(n);
    graph[0].emplace_back(1, 2);
    graph[0].emplace_back(2, 4);
    graph[1].emplace_back(2, 1);
    graph[1].emplace_back(3, 7);
    graph[2].emplace_back(3, 3);
    graph[3].emplace_back(4, 1);
    
    // 测试迪杰斯特拉算法
    cout << "=== 迪杰斯特拉算法 ===" << endl;
    vector<int> distDijkstra = dijkstra(graph, 0);
    printResult(distDijkstra, 0);
    
    // 测试贝尔曼-福特算法
    cout << "=== 贝尔曼-福特算法 ===" << endl;
    vector<int> distBellman;
    bool hasNegativeCycle = !bellmanFord(graph, 0, distBellman);
    if (hasNegativeCycle) {
        cout << "图中存在负权环，无法计算最短路径" << endl << endl;
    } else {
        printResult(distBellman, 0);
    }
    
    // 测试SPFA算法
    cout << "=== SPFA算法 ===" << endl;
    vector<int> distSPFA;
    bool hasNegativeCycleSPFA = !spfa(graph, 0, distSPFA);
    if (hasNegativeCycleSPFA) {
        cout << "图中存在负权环，无法计算最短路径" << endl << endl;
    } else {
        printResult(distSPFA, 0);
    }
    
    // 测试弗洛伊德算法
    cout << "=== 弗洛伊德算法 ===" << endl;
    vector<vector<int>> distFloyd = floydWarshall(graph);
    printAllPairsResult(distFloyd);
    
    // 测试含负权边的图
    cout << "=== 含负权边的图测试 ===" << endl;
    int m = 4;
    vector<vector<Edge>> graphWithNegative(m);
    graphWithNegative[0].emplace_back(1, 2);
    graphWithNegative[0].emplace_back(2, 5);
    graphWithNegative[1].emplace_back(2, -3);
    graphWithNegative[1].emplace_back(3, 1);
    graphWithNegative[2].emplace_back(3, 2);
    
    cout << "SPFA算法处理负权边:" << endl;
    vector<int> distSPFANegative;
    bool hasNegCycle = !spfa(graphWithNegative, 0, distSPFANegative);
    if (hasNegCycle) {
        cout << "图中存在负权环，无法计算最短路径" << endl;
    } else {
        printResult(distSPFANegative, 0);
    }
    
    return 0;
}
