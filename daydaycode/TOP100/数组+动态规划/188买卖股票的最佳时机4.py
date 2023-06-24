# 188. 买卖股票的最佳时机 IV


# 给定一个整数数组 prices ，它的第 i 个元素 prices[i] 是一支给定的股票在第 i 天的价格，和一个整型 k 。

# 设计一个算法来计算你所能获取的最大利润。你最多可以完成 k 笔交易。也就是说，你最多可以买 k 次，卖 k 次。

# 注意：你不能同时参与多笔交易（你必须在再次购买前出售掉之前的股票）。

 

# 示例 1：

# 输入：k = 2, prices = [2,4,1]
# 输出：2
# 解释：在第 1 天 (股票价格 = 2) 的时候买入，在第 2 天 (股票价格 = 4) 的时候卖出，这笔交易所能获得利润 = 4-2 = 2 。
# 示例 2：

# 输入：k = 2, prices = [3,2,6,5,0,3]
# 输出：7
# 解释：在第 2 天 (股票价格 = 2) 的时候买入，在第 3 天 (股票价格 = 6) 的时候卖出, 这笔交易所能获得利润 = 6-2 = 4 。
#      随后，在第 5 天 (股票价格 = 0) 的时候买入，在第 6 天 (股票价格 = 3) 的时候卖出, 这笔交易所能获得利润 = 3-0 = 3 。
class Solution:
    def maxProfit(self, k: int, prices: List[int]) -> int:
        ## 股票通用型动态规划
        ## dp[i] 第i天的最大利润
        ## dp[i][j] 第i天的最大利润,j是否持有，j=0表示不持有
        ## dp[i][k][j] 第i天的最大利润,j是否持有，j=0表示不持有，k表示已经交易几次
        n = len(prices)
        dp = [[[0 for _ in range(2)] for _ in range(k + 1)] for _ in range(n)]
        
        #print(dp)

        # 初始化 k = 0 表示 没有交易
        dp[0][0][0], dp[0][0][1] = 0, - prices[0]
        for idx in range(1, k + 1):
            dp[0][idx][0], dp[0][idx][1] = float('-inf'), float('-inf')

        for i in range(1, n):
            dp[i][0][0] = dp[i - 1][0][0]
            dp[i][0][1] = max(dp[i - 1][0][1], dp[i - 1][0][0] - prices[i])
            for k_indx in range(1, k + 1):
                # print(k)

                dp[i][k_indx][0] = max(dp[i - 1][k_indx][0], dp[i - 1][k_indx - 1][1] + prices[i])
                dp[i][k_indx][1] = max(dp[i- 1][k_indx][1], dp[i - 1][k_indx][0] - prices[i])

                # dp[i][2][0] = max(dp[i - 1][2][0], dp[i - 1][1][1] + prices[i])
                # dp[i][2][1] = max(dp[i - 1][2][1], dp[i - 1][2][0] - prices[i])
        res = 0
        for k_indx in range(k + 1):
            # print(dp[n -1][k_indx][0])
            res = max(res, dp[n -1][k_indx][0])
        # print(res)
        return res