# 15. 三数之和

# 给你一个整数数组 nums ，判断是否存在三元组 [nums[i], nums[j], nums[k]] 满足 i != j、i != k 且 j != k ，同时还满足 nums[i] + nums[j] + nums[k] == 0 。请

# 你返回所有和为 0 且不重复的三元组。

# 注意：答案中不可以包含重复的三元组。



# 示例 1：

# 输入：nums = [-1,0,1,2,-1,-4]
# 输出：[[-1,-1,2],[-1,0,1]]
# 解释：
# nums[0] + nums[1] + nums[2] = (-1) + 0 + 1 = 0 。
# nums[1] + nums[2] + nums[4] = 0 + 1 + (-1) = 0 。
# nums[0] + nums[3] + nums[4] = (-1) + 2 + (-1) = 0 。
# 不同的三元组是 [-1,0,1] 和 [-1,-1,2] 。
# 注意，输出的顺序和三元组的顺序并不重要。
# 示例 2：

# 输入：nums = [0,1,1]
# 输出：[]
# 解释：唯一可能的三元组和不为 0 。
# 示例 3：

# 输入：nums = [0,0,0]
# 输出：[[0,0,0]]
# 解释：唯一可能的三元组和为 0 。

## 三数之和什么解法都需要排序 排序为了剪枝去掉重复解

from collections import Counter
class Solution:
    def threeSum(self, nums: List[int]) -> List[List[int]]:
        # #  粗暴解法 两层遍历 
        # #  hash 
        # nums_count = Counter(nums)
        # nums = sorted(nums)
        # n = len(nums)
        # res = []
        
        # for i in range(n):
        #     nums_count[nums[i]] -= 1
        #     if i > 0 and nums[i] == nums[i - 1]:
        #         continue

        #     # 遍历第二个数 把遍历使用过的数字记录下来 避免出现使用已经使用过的解
        #     tmp_use = {}
        #     for j in range(i + 1, n):
        #         tmp_use[nums[j]] =  tmp_use.get(nums[j], 0) + 1
        #         if j > (i + 1) and nums[j] == nums[j - 1]:
        #             continue
        #         tmp_value = 0 - nums[i] - nums[j]
        #         if tmp_value in nums_count and nums_count[tmp_value] - tmp_use.get(tmp_value, 0) > 0:
        #             res.append([nums[i], nums[j], tmp_value])
        # return res

        ## 常见解法 双指针
        # 
        
            
            

