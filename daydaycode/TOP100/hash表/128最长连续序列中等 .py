# 128. 最长连续序列
# 给定一个未排序的整数数组 nums ，找出数字连续的最长序列（不要求序列元素在原数组中连续）的长度。

# 请你设计并实现时间复杂度为 O(n) 的算法解决此问题。

#  

# 示例 1：

# 输入：nums = [100,4,200,1,3,2]
# 输出：4
# 解释：最长数字连续序列是 [1, 2, 3, 4]。它的长度为 4。
# 示例 2：

# 输入：nums = [0,3,7,2,5,8,4,6,0,1]
# 输出：9
class Solution:
    def longestConsecutive(self, nums: List[int]) -> int:
        # 递归
        # nums_set = set(nums)
        # record = {}
        # def process(num):

        #     if num not in nums_set:
        #         return 0
            
        #     record[num] = process(num - 1) + 1
        #     return record[num]
        
        # hash
        nums_set = set(nums)
        res = 0
        for n in nums:
            # 从序列起始位置 or 最末端更新
            if n + 1 not in nums_set:
                cur_len = 1
                while n - 1 in nums_set:
                    cur_len += 1
                    n = n - 1
                res = max(res, cur_len)
        return res