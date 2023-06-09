# 123. 买卖股票的最佳时机 III

# 给定一个数组，它的第 i 个元素是一支给定的股票在第 i 天的价格。

# 设计一个算法来计算你所能获取的最大利润。你最多可以完成 两笔 交易。

# 注意：你不能同时参与多笔交易（你必须在再次购买前出售掉之前的股票）。

 

# 示例 1:

# 输入：prices = [3,3,5,0,0,3,1,4]
# 输出：6
# 解释：在第 4 天（股票价格 = 0）的时候买入，在第 6 天（股票价格 = 3）的时候卖出，这笔交易所能获得利润 = 3-0 = 3 。
#      随后，在第 7 天（股票价格 = 1）的时候买入，在第 8 天 （股票价格 = 4）的时候卖出，这笔交易所能获得利润 = 4-1 = 3 。
# 示例 2：

# 输入：prices = [1,2,3,4,5]
# 输出：4
# 解释：在第 1 天（股票价格 = 1）的时候买入，在第 5 天 （股票价格 = 5）的时候卖出, 这笔交易所能获得利润 = 5-1 = 4 。   
#      注意你不能在第 1 天和第 2 天接连购买股票，之后再将它们卖出。   
#      因为这样属于同时参与了多笔交易，你必须在再次购买前出售掉之前的股票。
# 示例 3：

# 输入：prices = [7,6,4,3,1] 
# 输出：0 
# 解释：在这个情况下, 没有交易完成, 所以最大利润为 0。
# 示例 4：

# 输入：prices = [1]
# 输出：0
class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        ## 股票通用型动态规划 和118题可以直接用一份代码 k = 2
        
        k  = 2

        n = len(prices)
        dp = [[[0 for _ in range(2)] for _ in range(k + 1)] for _ in range(n)]
        ## dp[i] 第i天的最大利润
        ## dp[i][j] 第i天的最大利润,j是否持有，j=0表示不持有
        ## dp[i][k][j] 第i天的最大利润,j是否持有，j=0表示不持有，k表示已经交易几次
        #print(dp)

        # 初始化 k = 0 表示没有交易
        # 第0天，没有交易，没有持有股票收益为0，持有股票收益为 - prices[0]
        dp[0][0][0], dp[0][0][1] = 0, - prices[0]
        for idx in range(1, k + 1):
            #print(idx)
            # 第0天，交易k次，没有持有股票收益为0，不可能持有股票，非法状态 初始化float('-inf')
            dp[0][idx][0], dp[0][idx][1] = 0, float('-inf')
        # dp[0][1][0], dp[0][1][1] = float('-inf'), float('-inf')
        # dp[0][2][0], dp[0][2][1] = float('-inf'), float('-inf')
        
        for i in range(1, n):
            dp[i][0][0] = dp[i - 1][0][0]
            dp[i][0][1] = max(dp[i - 1][0][1], dp[i - 1][0][0] - prices[i])
            for k_indx in range(1, k + 1):
                # print(k)

                # 当前未持有股票 = max（前一天未持有，前一天持有，今天卖出）
                dp[i][k_indx][0] = max(dp[i - 1][k_indx][0], dp[i - 1][k_indx - 1][1] + prices[i])

                # 当前持有股票 = max（前一天持有，前一天没有持有，今天买入）
                dp[i][k_indx][1] = max(dp[i- 1][k_indx][1], dp[i - 1][k_indx][0] - prices[i])

        res = 0
        for k_indx in range(k + 1):
            # print(dp[n -1][k_indx][0])
            res = max(res, dp[n -1][k_indx][0])
        # print(res)
        return res