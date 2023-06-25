# 55. 跳跃游戏

# 给定一个非负整数数组 nums ，你最初位于数组的 第一个下标 。

# 数组中的每个元素代表你在该位置可以跳跃的最大长度。

# 判断你是否能够到达最后一个下标。

 

# 示例 1：

# 输入：nums = [2,3,1,1,4]
# 输出：true
# 解释：可以先跳 1 步，从下标 0 到达下标 1, 然后再从下标 1 跳 3 步到达最后一个下标。
# 示例 2：

# 输入：nums = [3,2,1,0,4]
# 输出：false
# 解释：无论怎样，总会到达下标为 3 的位置。但该下标的最大跳跃长度是 0 ， 所以永远不可能到达最后一个下标。
from functools import lru_cache
class Solution:
    def canJump(self, nums: List[int]) -> bool:
        # # 递归  超出时间限制 有重复计算的部分
        
        # self.res = False
        # @lru_cache
        # def process(idx):
        #     if (idx + nums[idx]) >= len(nums) - 1:
        #         self.res = True
        #         return
        #     for i in range(idx + 1 , idx + nums[idx] + 1):
        #         # print(i)
        #         process(i)
        
        # n = len(nums) 
        # process(0)
        # return self.res

        # ## 基础动态规划
        # n = len(nums)
        # dp = [False for _ in range(n)]
        # dp[n - 1] = True

        # for i in range(n - 2,-1,-1):
        #     for idx in range(i + 1 , i + nums[i] + 1):
        #         if dp[idx]:
        #             dp[i] = True
        #             break
        # return dp[0]

        ## 贪心算法
        # 维持一个当前最远距离
        max_len = nums[0]
        n = len(nums)
        for i in range(n):
            # 当前点 可以达到 更新最远距离
            if i <= max_len:
                max_len = max(max_len, i + nums[i])
                
                # 最远距离可以到达最后一位 返回True
                if max_len >= n - 1:
                    return True
        
        return False

