# 279. 完全平方数



# 给你一个整数 n ，返回 和为 n 的完全平方数的最少数量 。

# 完全平方数 是一个整数，其值等于另一个整数的平方；换句话说，其值等于一个整数自乘的积。例如，1、4、9 和 16 都是完全平方数，而 3 和 11 不是。

 

# 示例 1：

# 输入：n = 12
# 输出：3 
# 解释：12 = 4 + 4 + 4
# 示例 2：

# 输入：n = 13
# 输出：2
# 解释：13 = 4 + 9
 
# 提示：

# 1 <= n <= 104
class Solution:
    
    def numSquares(self, n: int) -> int:
    #     #暴力递归
    #     @lru_cache(None)
    #     def process(n):
    #         # base case
    #         if n < 0:
    #             return 0
    #         ceil = int(math.sqrt(n))
    #         if n == ceil ** 2:
    #             return 1
    #         res = n
    #         for c in range(ceil, 1, -1):
    #             cur = 1 + process(n - c**2)
    #             res = min(cur, res)
    #         return res

    #     return process(n)

        # # 动态规划
        if n < 0:
            return 0
        # if n < 3:
        #     return n
        dp = [float('inf')] * (n + 1)
        # 
        # dp[0], dp[1], dp[2] = 1, 1, 2
        for num in range(n + 1):
            ceil = int(math.sqrt(num))
            for c in range(ceil, 0, -1):
                rest = num - c**2
                if rest == 0 and c == ceil:
                    dp[num] = 1
                    continue
                if rest >= 0:
                    dp[num] = min(dp[num], 1 + dp[rest])
        # print(dp)
        return dp[n]