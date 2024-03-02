# 54. 螺旋矩阵
# 中等
# 相关标签
# 相关企业
# 提示
# 给你一个 m 行 n 列的矩阵 matrix ，请按照 顺时针螺旋顺序 ，返回矩阵中的所有元素。

 

# 示例 1：


# 输入：matrix = [[1,2,3],[4,5,6],[7,8,9]]
# 输出：[1,2,3,6,9,8,7,4,5]
# 示例 2：


# 输入：matrix = [[1,2,3,4],[5,6,7,8],[9,10,11,12]]
# 输出：[1,2,3,4,8,12,11,10,9,5,6,7]
 

# 提示：

# m == matrix.length
# n == matrix[i].length
# 1 <= m, n <= 10
# -100 <= matrix[i][j] <= 100

class Solution:
    def spiralOrder(self, matrix: List[List[int]]) -> List[int]:
        # 先i+1,j+1,i-1,j+1
        [[0, 1], [1, 0], [0, -1], [-1, 0]]

        m, n = len(matrix), len(matrix[0])
        ways = [[0, 1], [1, 0], [0, -1], [-1, 0]]
        visted = [[False] * n for _ in range(m)]
        idx = 0
        i, j = 0, 0
        res = [0] * m * n

        for k in range(m * n):
            res[k] = matrix[i][j]
            visted[i][j] = True
            
            # 判断下一状态是否合法 如果越界 or 已经访问过 更新方向
            new_i, new_j = i + ways[idx][0], j + ways[idx][1]
            if not (0<= new_i < m and 0<= new_j < n  and not visted[new_i][new_j]):
                idx = (idx + 1) % 4
            
            i += ways[idx][0]
            j += ways[idx][1]
        
        return res
        