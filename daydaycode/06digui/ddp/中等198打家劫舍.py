# 198. 打家劫舍


# 你是一个专业的小偷，计划偷窃沿街的房屋。每间房内都藏有一定的现金，影响你偷窃的唯一制约因素就是相邻的房屋装有相互连通的防盗系统，如果两间相邻的房屋在同一晚上被小偷闯入，系统会自动报警。

# 给定一个代表每个房屋存放金额的非负整数数组，计算你 不触动警报装置的情况下 ，一夜之内能够偷窃到的最高金额。

 

# 示例 1：

# 输入：[1,2,3,1]
# 输出：4
# 解释：偷窃 1 号房屋 (金额 = 1) ，然后偷窃 3 号房屋 (金额 = 3)。
#      偷窃到的最高金额 = 1 + 3 = 4 。
# 示例 2：

# 输入：[2,7,9,3,1]
# 输出：12
# 解释：偷窃 1 号房屋 (金额 = 2), 偷窃 3 号房屋 (金额 = 9)，接着偷窃 5 号房屋 (金额 = 1)。
#      偷窃到的最高金额 = 2 + 9 + 1 = 12 。
# 提示：

# 1 <= nums.length <= 100
# 0 <= nums[i] <= 400

class Solution:
    def rob(self, nums: List[int]) -> int:
        # # 暴力递归
        # def process(nums, index, issteal):
        #     # base case 越界
        #     if index < 0 :
        #         return 0

        #     if issteal:
        #         # 这家可偷
        #         return max(nums[index] + process(nums, index - 1, 0), process(nums, index - 1, 1))
        #     else:
        #         # 这家不可偷
        #         return process(nums, index - 1, 1)
        # return process(nums, len(nums) - 1, 1)

        ## 暴力递归改动态规划
        # # dp = [[0 for _ in range(2)] for _ in range(len(nums))]
        # # dp[0][1] = nums[0]

        # for i in range(1, len(nums)):
        #     
        #     # dp[i][1] = max(nums[i] + dp[i - 1][0], dp[i - 1][1])
        #     # dp[i][0] = dp[i - 1][1]

        # return dp[len(nums) - 1][1]

        ### 动态规划优化空间复杂度
        pre_no = 0
        pre_yes = nums[0]
        for i in range(1, len(nums)):
            pre_yes, pre_no = max(nums[i] + pre_no, pre_yes), pre_yes
        return pre_yes
