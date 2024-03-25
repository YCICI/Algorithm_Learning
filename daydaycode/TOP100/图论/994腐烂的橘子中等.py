# 
class Solution:
    def orangesRotting(self, grid: List[List[int]]) -> int:
         # bfs 第一步把第0分钟的腐烂的橘子放入队列（i, j , d）i,j表示位置 d表示第几分钟
        # 最后遍历一遍所有的橘子 如果有没有腐烂的 返回-1

        m, n = len(grid), len(grid[0])
        queue = collections.deque()

        # 初始化
        for i in range(m):
            for j in range(n):
                if grid[i][j] == 2:
                    queue.append([i, j, 0])

        ways = [[1, 0], [-1, 0], [0, 1], [0, -1]]
        d = 0
        while queue:
            r, c, d = queue.popleft()
            for way in ways:
                new_r, new_c = r + way[0], c + way[1]
                if 0<= new_r < m and 0 <= new_c < n and grid[new_r][new_c] == 1:
                    grid[new_r][new_c] = 2
                    queue.append([new_r, new_c, d + 1])
        
        if any(1 in row for row in grid):
            return -1
        
        return d

