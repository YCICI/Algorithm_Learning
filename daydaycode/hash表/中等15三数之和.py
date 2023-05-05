# 15. 三数之和
# 给你一个包含 n 个整数的数组 nums，判断 nums 中是否存在三个元素 a，b，c ，使得 a + b + c = 0 ？请你找出所有和为 0 且不重复的三元组。
#
# 注意：答案中不可以包含重复的三元组。
#
#
#
# 示例 1：
#
# 输入：nums = [-1,0,1,2,-1,-4]
# 输出：[[-1,-1,2],[-1,0,1]]
# 示例 2：
#
# 输入：nums = []
# 输出：[]
# 示例 3：
#
# 输入：nums = [0]
# 输出：[]
#
#
# 提示：
#
# 0 <= nums.length <= 3000
# -105 <= nums[i] <= 105
# ************* 暴力解法 maphash************* #
class Solution:
    def threeSum(self, nums):

        
        res = set()
        nums.sort()
        
        # 固定第一个数
        for i in range(len(nums)):
            if i > 0 and nums[i] == nums[i-1]:
                continue
            num_map = {}
            # 
            for j in range(i, len(nums)):
                if nums[j] in num_map:
                    res.add((num_map[nums[j]][0], num_map[nums[j]][1], nums[j]))
                
                # 按第3个数作为key 记录其他两个数
                num_map[0 - nums[i] - nums[j]] = [nums[i], nums[j]]
        print(num_map)
        
        return list(res)
    

nums = [-1,0,1,2,-1,-4]
s = Solution()
res = s.threeSum(nums)
print(list(res))



