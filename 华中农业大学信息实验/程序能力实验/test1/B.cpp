#include <queue>
#include <iostream>
#include <climits>
using namespace std;
struct jihua
{
    int index;
    int tianshu;
    int ziyuan;

    jihua() {}
    jihua(int i, int t, int c) : index(i), tianshu(t), ziyuan(c) {}
};
int main()
{
    ios::sync_with_stdio(false);
    cout.tie(NULL);
    cin.tie(NULL);
    auto cmp = [&](jihua &a, jihua &b)
    {
        if (a.tianshu < b.tianshu)
        {
            return true;
        }
        else if (a.tianshu > b.tianshu)
        {
            return false;
        }
        return a.ziyuan > b.ziyuan;
    };
    priority_queue<jihua, vector<jihua>, decltype(cmp)> pq(cmp);

    int n, m, k;
    cin >> n >> m >> k;
    int t, c;
    for (int i = 0; i < n; ++i)
    {
        cin >> t >> c;
        pq.emplace(jihua(i, t, c));
    }
    int max_tian = pq.top().tianshu;
    while (1)
    {
        if (m < pq.top().ziyuan)
        {
            max_tian = min(max_tian, pq.top().tianshu);
            break;
        }
        if (pq.top().tianshu <= k)
        {
            max_tian = min(max_tian, pq.top().tianshu);
            break;
        }

        auto [index, tianshu, ziyuan] = pq.top();
        max_tian = min(tianshu, max_tian);
        pq.pop();
        m -= ziyuan;
        pq.emplace(jihua(index, tianshu - 1, ziyuan));
    }
    cout << max_tian << endl;

    return 0;
}