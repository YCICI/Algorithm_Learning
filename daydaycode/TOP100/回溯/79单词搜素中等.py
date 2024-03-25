# 79. 单词搜索
# 已解答
# 中等
# 相关标签
# 相关企业
# 给定一个 m x n 二维字符网格 board 和一个字符串单词 word 。如果 word 存在于网格中，返回 true ；否则，返回 false 。

# 单词必须按照字母顺序，通过相邻的单元格内的字母构成，其中“相邻”单元格是那些水平相邻或垂直相邻的单元格。同一个单元格内的字母不允许被重复使用。

 

# 示例 1：


# 输入：board = [["A","B","C","E"],["S","F","C","S"],["A","D","E","E"]], word = "ABCCED"
# 输出：true
# 示例 2：


# 输入：board = [["A","B","C","E"],["S","F","C","S"],["A","D","E","E"]], word = "SEE"
# 输出：true
# 示例 3：


# 输入：board = [["A","B","C","E"],["S","F","C","S"],["A","D","E","E"]], word = "ABCB"
# 输出：false
 

# 提示：

# m == board.length
# n = board[i].length
# 1 <= m, n <= 6
# 1 <= word.length <= 15
# board 和 word 仅由大小写英文字母组成
class Solution:
    def exist(self, board: List[List[str]], word: str) -> bool:

        # ways = [[1, 0], [-1, 0], [0, 1], [0, -1]]
        # # self.flag = False
        # m, n = len(board), len(board[0])
        # visted = set()

        # def process(i, j, idx, visted):
            
        #     if board[i][j] != word[idx]:
        #         return False
            
        #     if idx == len(word) - 1:
        #         return True

        #     visted.add((i, j))
        #     result = False
        #     for way in ways:
        #         new_i, new_j = i + way[0], j + way[1]
        #         if 0 <= new_i < m and 0 <= new_j < n :
        #             if (new_i, new_j) not in visted:
        #                 if process(new_i, new_j, idx + 1, visted):
        #                     result = True
        #                     break

        #     visted.remove((i, j))
        #     return result
        
        # for i in range(m):
        #     for j in range(n):
        #         if process(i, j, 0, visted):
        #             return True
        
        # return False


        ways = [[1, 0], [-1, 0], [0, 1], [0, -1]]
        m, n = len(board), len(board[0])
        visited = [[0] * n for _ in range(m)]
        lent = len(word)
        self.flag = False

        def process(i, j, idx):
            # print(i, j, idx)
            if board[i][j] != word[idx]:
                return
            
            if idx == lent - 1:
                self.flag = True
                return
            
            visited[i][j] = 1
            for way in ways:
                new_i, new_j = i + way[0], j + way[1]
                if 0<= new_i < m and 0<= new_j < n and visited[new_i][new_j] == 0:
                    process(new_i, new_j, idx + 1)
            visited[i][j] = 0
            return 

        for i in range(m):
            for j in range(n):
                if not self.flag and board[i][j] == word[0]:
                    visited = [[0] * n for _ in range(m)]
                    process(i, j, 0)
        return self.flag            
            

