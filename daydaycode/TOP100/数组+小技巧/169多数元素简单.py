# 169. 多数元素
# 简单
# 2K
# 相关企业
# 给定一个大小为 n 的数组 nums ，返回其中的多数元素。多数元素是指在数组中出现次数 大于 ⌊ n/2 ⌋ 的元素。

# 你可以假设数组是非空的，并且给定的数组总是存在多数元素。

 

# 示例 1：

# 输入：nums = [3,2,3]
# 输出：3
# 示例 2：

# 输入：nums = [2,2,1,1,1,2,2]
# 输出：2
class Solution:
    def majorityElement(self, nums: List[int]) -> int:
        # 基础解法1 hash表
        counts = collections.Counter(nums)
        return max(counts.keys(), key=counts.get)


        # 基础解法2 排序 返回 n//2

        # 最优解法 投票法
        # candi = nums[0]
        # counts = 0
        # for num in nums:
        #     if counts == 0:
        #         candi = num
        #     counts += (1 if num == candi else -1)
        # return candi