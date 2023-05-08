# 560. 和为 K 的子数组
# 给你一个整数数组 nums 和一个整数 k ，请你统计并返回 该数组中和为 k 的连续子数组的个数 。
#
#
#
# 示例 1：
#
# 输入：nums = [1,1,1], k = 2
# 输出：2
# 示例 2：
#
# 输入：nums = [1,2,3], k = 3
# 输出：2
#
#
# 提示：
#
# 1 <= nums.length <= 2 * 104
# -1000 <= nums[i] <= 1000
# -107 <= k <= 107
class Solution:
# ******************** 暴力解法一 时间O(n2) ******************** #
    def subarraySum1(self, nums, k: int) -> int:
        # 字典记录字数组 
        res = 0
        for i in range(len(nums)):
            for j in range(i , len(nums)):
                #print(i, j)
                pre_sum = sum(nums[i:j + 1])
                #print(pre_sum)
                if pre_sum == k:
                    res += 1
        return res

# ******************** 优化 时间O(n) ******************** #   
    def subarraySum2(self, nums, k: int) -> int:
        # 暴力求解的方法 可以看出实际是在求 sum(nums[i:j + 1]) = K  -> sum(nums[:j]) - sum(nums[:i]) = K -> pre_num[j] - pre_num[i] = K
        # pre_nums 表示前缀和
        pre_num = 0
        res = 0
        pre_num_map = {0: 1}
        for i in range(len(nums)):
            pre_num += nums[i] 
            res += pre_num_map.get(pre_num - k , 0) # pre_num_j - k = pre_num_i 满足条件的数组组合

            pre_num_map[pre_num] = pre_num_map.get(pre_num, 0) + 1
        
        return res
    
f = Solution()
nums = [1,2,3,2,1,1]
K = 3
res1 = f.subarraySum1(nums, K)
res2 = f.subarraySum2(nums, K)
print(res1)
print(res2)