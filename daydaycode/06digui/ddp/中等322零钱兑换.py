# 322. 

# 给你一个整数数组 coins ，表示不同面额的硬币；以及一个整数 amount ，表示总金额。

# 计算并返回可以凑成总金额所需的 最少的硬币个数 。如果没有任何一种硬币组合能组成总金额，返回 -1 。

# 你可以认为每种硬币的数量是无限的。

 

# 示例 1：

# 输入：coins = [1, 2, 5], amount = 11
# 输出：3 
# 解释：11 = 5 + 5 + 1
# 示例 2：

# 输入：coins = [2], amount = 3
# 输出：-1
# 示例 3：

# 输入：coins = [1], amount = 0
# 输出：0
 

# 提示：

# 1 <= coins.length <= 12
# 1 <= coins[i] <= 231 - 1
# 0 <= amount <= 104

from functools import lru_cache
class Solution:
    def coinChange(self, coins: List[int], amount: int) -> int:
        
        # # 暴力递归
        # @lru_cache
        # def process(res, cnt):

        #     if res == 0:
        #         self.cnt_init = min(self.cnt_init, cnt)
        #         return 
            
        #     for coin in self.sorted_coins:
        #         if coin > res:
        #             break
        #         process(res - coin, cnt + 1)
        #     return 
         
        # self.cnt_init = float('inf')
        # self.sorted_coins = sorted(coins)
        # process(amount,  0)
        # return self.cnt_init if self.cnt_init != float('inf') else -1
        # 动态规划
        # 
        dp = [float('inf') for _ in range(amount + 1)]
        dp[0] = 0
        #print(dp)
        
        sorted_coins = sorted(coins)
        for i in range(1, amount + 1):
            for coin in sorted_coins:
                if coin > i:
                    break
                dp[i] = min(dp[i], dp[i - coin]+ 1)
        # print(dp)
        return dp[amount] if dp[amount] != float('inf') else -1

             
