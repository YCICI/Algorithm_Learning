# 1143. 最长公共子序列
# 给定两个字符串 text1 和 text2，返回这两个字符串的最长 公共子序列 的长度。如果不存在 公共子序列 ，返回 0 。
#
# 一个字符串的 子序列 是指这样一个新的字符串：它是由原字符串在不改变字符的相对顺序的情况下删除某些字符（也可以不删除任何字符）后组成的新字符串。
#
# 例如，"ace" 是 "abcde" 的子序列，但 "aec" 不是 "abcde" 的子序列。
# 两个字符串的 公共子序列 是这两个字符串所共同拥有的子序列。
#
#
#
# 示例 1：
#
# 输入：text1 = "abcde", text2 = "ace"
# 输出：3
# 解释：最长公共子序列是 "ace" ，它的长度为 3 。
# 示例 2：
#
# 输入：text1 = "abc", text2 = "abc"
# 输出：3
# 解释：最长公共子序列是 "abc" ，它的长度为 3 。
# 示例 3：
#
# 输入：text1 = "abc", text2 = "def"
# 输出：0
# 解释：两个字符串没有公共子序列，返回 0 。
#
#
# 提示：
#
# 1 <= text1.length, text2.length <= 1000
# text1 和 text2 仅由小写英文字符组成。

# ***************** 动态规划 ***************** #

class Solution:
    def longestCommonSubsequence(self, text1: str, text2: str) -> int:
        # 动态规划
        dp = [[0 for _ in range(len(text1))] for _ in range(len(text2)) ]

        # base case
        if text1[0] == text2[0]:
            dp[0][0] = 1
        for i in range(1, len(text2)):
            if text2[i] == text1[0]:
                dp[i][0] = 1
            dp[i][0] = max(dp[i - 1][0], dp[i][0])
        for j in range(1, len(text1)):
            if text1[j] == text2[0]:
                dp[0][j] = 1
            dp[0][j]= max(dp[0][j - 1], dp[0][j])

        # 递归 分情况
        # dp[i][j]表示 str[..i] 和 str[..j]结尾的最长公共子序列
        for i in range(1, len(text2)):
            for j in range(1, len(text1)):
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
                if text2[i] == text1[j]:
                    dp[i][j] = max(dp[i][j], dp[i - 1][j - 1] + 1)
        
        return dp[len(text2) - 1][len(text1) - 1]