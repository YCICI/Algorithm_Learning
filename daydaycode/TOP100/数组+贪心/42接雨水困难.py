# 42. 接雨水


# 给定 n 个非负整数表示每个宽度为 1 的柱子的高度图，计算按此排列的柱子，下雨之后能接多少雨水。

 

# 示例 1：



# 输入：height = [0,1,0,2,1,0,1,3,2,1,2,1]
# 输出：6
# 解释：上面是由数组 [0,1,0,2,1,0,1,3,2,1,2,1] 表示的高度图，在这种情况下，可以接 6 个单位的雨水（蓝色部分表示雨水）。 
# 示例 2：

# 输入：height = [4,2,0,3,2,5]
# 输出：9
class Solution:
    def trap(self, height: List[int]) -> int:
         # 解法一 动态规划
        # res[i]表示i位置能接的最大雨水，那么res[i] 取决于左边最大高度，右边最大高度，和i位置自己的高度
        # 即res[i] = min(maxleft[i], maxright[i]) - height[i]
        # 问题变成记录左右最大高度，暴力解法是分别左右遍历 优化解法是一次遍历记录下左右最大高度
        n = len(height)
        max_left = [0] * n
        max_right = [0] * n
        # 
        max_left[0] = height[0]
        max_right[n - 1] = height[n - 1]
        for i in range(1,n):
            max_left[i] = max(max_left[i - 1], height[i])
        
        for j in range(n - 2, -1, -1):
            max_right[j] = max(max_right[j + 1], height[j])
        
        # 
        res = 0
        for idx in range(n):
            res += min(max_left[idx], max_right[idx]) - height[idx]
        
        return res



        # 解法二 动态规划进一步优化 空间复杂度压缩至O(1)

       


        