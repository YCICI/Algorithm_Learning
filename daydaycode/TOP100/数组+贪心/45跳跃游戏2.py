# 45. 跳跃游戏 II


# 给定一个长度为 n 的 0 索引整数数组 nums。初始位置为 nums[0]。

# 每个元素 nums[i] 表示从索引 i 向前跳转的最大长度。换句话说，如果你在 nums[i] 处，你可以跳转到任意 nums[i + j] 处:

# 0 <= j <= nums[i] 
# i + j < n
# 返回到达 nums[n - 1] 的最小跳跃次数。生成的测试用例可以到达 nums[n - 1]。

 

# 示例 1:

# 输入: nums = [2,3,1,1,4]
# 输出: 2
# 解释: 跳到最后一个位置的最小跳跃数是 2。
#      从下标为 0 跳到下标为 1 的位置，跳 1 步，然后跳 3 步到达数组的最后一个位置。
# 示例 2:

# 输入: nums = [2,3,0,1,4]
# 输出: 2
 

# 提示:

# 1 <= nums.length <= 104
# 0 <= nums[i] <= 1000
# 题目保证可以到达 nums[n-1]
class Solution:
    def jump(self, nums: List[int]) -> int:
        # 动态规划
        # n = len(nums)
        # dp = [float('inf') for _ in range(n)]
        # dp[n - 1] = 0

        # #
        # for i in range(n - 2, -1, -1):
        #     # print("i", i)
        #     if nums[i] >= 1:
        #         dp[i] = 1 + dp[i + 1]

        #     if nums[i] + i >= n - 1:
        #         dp[i] = 1
        #     # print(dp)
        #     for idx in range(i + 1, i + nums[i] + 1):
        #         if i + nums[i] + 1 < n:
        #             dp[i] = min(dp[i], 1 + dp[idx])

        # return dp[0]
        # 贪心算法 维护能到达的最远距离 
        # [2,3,1,1,4] 2能到达的范围是3，1的位置，3位置能到达的距离更远，维持3位置能到达的距离为下一次的起点
        n = len(nums)
        step, end, max_len =0, 0, 0
        for i in range(n - 1):
            # 更新能到的最远距离
            if max_len >= i:
                max_len = max(max_len, i + nums[i])
                
                # 能到最远距离的i 作为新的起点
                if i == end:
                    end = max_len
                    step += 1
        
        return step

