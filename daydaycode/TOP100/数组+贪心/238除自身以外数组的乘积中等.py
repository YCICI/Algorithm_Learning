# 238. 除自身以外数组的乘积


# 给你一个整数数组 nums，返回 数组 answer ，其中 answer[i] 等于 nums 中除 nums[i] 之外其余各元素的乘积 。

# 题目数据 保证 数组 nums之中任意元素的全部前缀元素和后缀的乘积都在  32 位 整数范围内。

# 请不要使用除法，且在 O(n) 时间复杂度内完成此题。

 

# 示例 1:

# 输入: nums = [1,2,3,4]
# 输出: [24,12,8,6]
# 示例 2:

# 输入: nums = [-1,1,0,-3,3]
# 输出: [0,0,9,0,0]
 

# 提示：

# 2 <= nums.length <= 105
# -30 <= nums[i] <= 30
# 保证 数组 nums之中任意元素的全部前缀元素和后缀的乘积都在  32 位 整数范围内
class Solution:
    def productExceptSelf(self, nums: List[int]) -> List[int]:
        
        # 贪心解
        # 当前索引i位置的乘积  = 索引左侧乘积 * 索引右侧乘积
        # L[i] = L[i - 1] * nums[i - 1]
        # R[i] = R[i + 1] * nums[i + 1]

        n = len(nums)
        L = [0 for _ in range(n)]
        R = [0 for _ in range(n)]

        # 
        L[0] = 1
        R[n - 1] = 1
        for i in range(1, n):
            #print(i)
            L[i] = L[i - 1] * nums[i - 1]
        for j in range(n - 2, -1, -1):
            #print(j)
            R[j] = R[j + 1] * nums[j + 1]
        
        # print(L,R)
        answer = []
        for i in range(n):
            cur = L[i] * R[i]
            answer.append(cur)
        return answer



        
 