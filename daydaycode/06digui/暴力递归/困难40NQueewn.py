# 按照国际象棋的规则，皇后可以攻击与之处在同一行或同一列或同一斜线上的棋子。

# n 皇后问题 研究的是如何将 n 个皇后放置在 n×n 的棋盘上，并且使皇后彼此之间不能相互攻击。

# 给你一个整数 n ，返回所有不同的 n 皇后问题 的解决方案。

# 每一种解法包含一个不同的 n 皇后问题 的棋子放置方案，该方案中 'Q' 和 '.' 分别代表了皇后和空位。

#  

# 示例 1：


# 输入：n = 4
# 输出：[[".Q..","...Q","Q...","..Q."],["..Q.","Q...","...Q",".Q.."]]
# 解释：如上图所示，4 皇后问题存在两个不同的解法。
# 示例 2：

# 输入：n = 1
# 输出：[["Q"]]
# leetcode submit region begin(Prohibit modification and deletion)

### ********************* 解法一 暴力递归 ********************* ### 
class Solution:
    def solveNQueens(self, n: int):
        
        def process(i, record, n):
            # base case
            if i == n:
                # self.res.append("." * col + "Q" + "."* (n - col -1) for col in index)
                return self.res.append(["." * col + "Q" + "."* (n - col -1) for col in record])
            # 放置i行的Queen
            for j in range(n):
                if isvaild(i, j, record):
                    record[i] = j
                    process(i+1, record, n)
                    record[i] = 0
            return 
        
        def isvaild(i, j, record):
            # 不同行，不同列，不同对角线
            for k in range(i):
                # 第k行的皇后 （k, record[k]） 第i行的皇后（i，j）
                if (record[k] == j) or (abs(record[k] - j) == abs(i - k)):
                    return False         
            return True
        
        # 
        record = [0] * n
        self.res = []
        process(0, record, n)
        # print(self.res )
        return self.res 
        
S = Solution()
result = S.solveNQueens(4)
print(result)


### ********************* 解法二 暴力递归 + 位运算优化 ********************* ### 