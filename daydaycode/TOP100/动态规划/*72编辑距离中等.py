# 72. 编辑距离
# 已解答
# 中等
# 相关标签
# 相关企业
# 给你两个单词 word1 和 word2， 请返回将 word1 转换成 word2 所使用的最少操作数  。

# 你可以对一个单词进行如下三种操作：

# 插入一个字符
# 删除一个字符
# 替换一个字符
 

# 示例 1：

# 输入：word1 = "horse", word2 = "ros"
# 输出：3
# 解释：
# horse -> rorse (将 'h' 替换为 'r')
# rorse -> rose (删除 'r')
# rose -> ros (删除 'e')
# 示例 2：

# 输入：word1 = "intention", word2 = "execution"
# 输出：5
# 解释：
# intention -> inention (删除 't')
# inention -> enention (将 'i' 替换为 'e')
# enention -> exention (将 'n' 替换为 'x')
# exention -> exection (将 'n' 替换为 'c')
# exection -> execution (插入 'u')

class Solution:
    def minDistance(self, word1: str, word2: str) -> int:
        # dp[i][j] 表示i结尾的word1变成j结尾的word2的最小编辑次数
        n1, n2 = len(word1), len(word2)
        dp = [[float('inf')] * (n1 + 1) for _ in range(n2 + 1)]
        # print(dp)
        for i in range(n1 + 1):
            dp[0][i] = i
        for j in range(n2 + 1):
            dp[j][0] = j

        # 动态转移
        for i in range(1, n2+1):
            for j in range(1, n1+1):
                ## word2 插入一个字符
                left = dp[i][j - 1] + 1 ##
                # word1 插入一个字符
                up = dp[i - 1][j] + 1
                # 修改一个字符
                up_left = dp[i - 1][j - 1]

                if word1[j - 1] != word2[i - 1]:
                    up_left = up_left + 1
                dp[i][j] = min(left, up, up_left)
            
        return dp[-1][-1]
        



