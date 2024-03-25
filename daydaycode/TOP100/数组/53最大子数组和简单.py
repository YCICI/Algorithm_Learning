# 53. 最大子数组和
# 已解答
# 中等
# 相关标签
# 相关企业
# 给你一个整数数组 nums ，请你找出一个具有最大和的连续子数组（子数组最少包含一个元素），返回其最大和。

# 子数组
# 是数组中的一个连续部分。

 

# 示例 1：

# 输入：nums = [-2,1,-3,4,-1,2,1,-5,4]
# 输出：6
# 解释：连续子数组 [4,-1,2,1] 的和最大，为 6 。
# 示例 2：

# 输入：nums = [1]
# 输出：1
# 示例 3：

# 输入：nums = [5,4,-1,7,8]
# 输出：23
 

# 提示：

# 1 <= nums.length <= 105
# -104 <= nums[i] <= 104
 

# 进阶：如果你已经实现复杂度为 O(n) 的解法，尝试使用更为精妙的 分治法 求解。
class Solution:
    def maxSubArray(self, nums: List[int]) -> int:
        # 方法一 前缀和 解题思路参照 560 和为k的子数组 或者 更新结构可以参考 121 买卖股票的最佳时机
        # 
        # ans = float('-inf')
        # n = len(nums)
        # min_pre, pre = 0, 0

        # for i in range(n):
        #     pre += nums[i]
        #     ans = max(ans, pre - min_pre)
        #     min_pre = min(min_pre, pre)
            
        # return ans
        # 方法二 动态规划
        n = len(nums)
        ans = nums[0]
        pre = 0

        for i in range(n):
            
            pre = max(pre + nums[i], nums[i])
            ans = max(ans, pre)
        return ans




        
