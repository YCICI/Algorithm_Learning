# 42. 接雨水
# 困难
# 相关标签
# 相关企业
# 给定 n 个非负整数表示每个宽度为 1 的柱子的高度图，计算按此排列的柱子，下雨之后能接多少雨水。

 

# 示例 1：



# 输入：height = [0,1,0,2,1,0,1,3,2,1,2,1]
# 输出：6
# 解释：上面是由数组 [0,1,0,2,1,0,1,3,2,1,2,1] 表示的高度图，在这种情况下，可以接 6 个单位的雨水（蓝色部分表示雨水）。 
# 示例 2：

# 输入：height = [4,2,0,3,2,5]
# 输出：9
 

# 提示：

# n == height.length
# 1 <= n <= 2 * 104
# 0 <= height[i] <= 105
class Solution:
    def trap(self, height: List[int]) -> int:
        # 动态规划
        # 更新两个数组 一个是左边的最大高度 一个是右边的最大高度 每个位置能接的水的最大高度=左右两边高度最小值
        # 遍历每个位置能接雨水的量 = min（左高，右高）-heigh(i)
        if not height:
            return 0
        n = len(height)
        left_max = [0] * n
        left_max[0] = height[0]
        for i in range(1, n):
            left_max[i] = max(left_max[i - 1], height[i])
        
        right_max = [0] * n
        right_max[n - 1] = height[n - 1]
        for i in range(n - 2, -1, -1):
            right_max[i] = max(right_max[i + 1], height[i])


        ans = sum([min(left_max[i], right_max[i]) - height[i] for i in range(n)])
        return ans
        

