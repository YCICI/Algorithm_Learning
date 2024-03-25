# 131. 分割回文串
# 中等
# 相关标签
# 相关企业
# 给你一个字符串 s，请你将 s 分割成一些子串，使每个子串都是 
# 回文串
#  。返回 s 所有可能的分割方案。

 

# 示例 1：

# 输入：s = "aab"
# 输出：[["a","a","b"],["aa","b"]]
# 示例 2：

# 输入：s = "a"
# 输出：[["a"]]
 

# 提示：

# 1 <= s.length <= 16
# s 仅由小写英文字母组成
class Solution:
    def partition(self, s: str) -> List[List[str]]:



        n = len(s)
        dp = [[True]*n for _ in range(n)]

        for i in range(n - 1, -1, -1):
            for j in range(i + 1, n):
                dp[i][j] = (s[i] == s[j]) and (dp[i - 1][j - 1])
        
        self.res = []

        def process(i, path):
            if i == n:
                self.res.append(path[:])
                return
            
            for j in range(i, n):
                if dp[i][j]:
                    path.append(s[i:j+1])
                    process(j + 1, path)
                    path.pop()
            return
        
        process(0, [])
        return self.res