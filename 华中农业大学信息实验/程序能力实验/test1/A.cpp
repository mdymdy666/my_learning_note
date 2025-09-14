#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;
const int maxn = static_cast<int>(1e4 + 10);

int main()
{
    ios::sync_with_stdio(false);
    cin.tie(NULL);
    cout.tie(NULL);
    int n, a, b;
    cin >> n;
    cin >> a >> b;
    
    vector<vector<int>> chafen(a + 2, vector<int>(b + 2, 0));
    int x1, y1, x2, y2;

    for (int i = 0; i < n; ++i)
    {
        cin >> x1 >> y1 >> x2 >> y2;
        --x2;
        --y2;
        if (x1 >= a)
            continue;
        if (y1 >= b)
            continue;
        if (x2 < 0)
            continue;
        if (y2 < 0)
            continue;
        x1 = max(0, x1);
        y1 = max(0, y1);
        x2 = min(a - 1, x2);
        y2 = min(b - 1, y2);
        x1++, y1++, x2++, y2++;

        chafen[x1][y1]++;
        chafen[x1][y2 + 1]--;
        chafen[x2 + 1][y1]--;
        chafen[x2 + 1][y2 + 1]++;
    }
    for (int i = 1; i <= a; ++i)
    {
        for (int j = 1; j <= b; ++j)
        {
            chafen[i][j] += chafen[i - 1][j];
        }
    }
    for (int i = 1; i <= a; ++i)
    {
        for (int j = 1; j <= b; ++j)
        {
            chafen[i][j] += chafen[i][j - 1];
        }
    }

    long long cnt = 0;

    for (int i = 1; i <= a; ++i)
    {
        for (int j = 1; j <= b; ++j)
        {
            if (chafen[i][j] != 0)
            {
                cnt++;
            }
        }
    }
    cout << cnt;
    return 0;
}