# 560. 和为 K 的子数组
# 中等
# 相关标签
# 相关企业
# 提示
# 给你一个整数数组 nums 和一个整数 k ，请你统计并返回 该数组中和为 k 的子数组的个数 。

# 子数组是数组中元素的连续非空序列。

 

# 示例 1：

# 输入：nums = [1,1,1], k = 2
# 输出：2
# 示例 2：

# 输入：nums = [1,2,3], k = 3
# 输出：2
 

# 提示：

# 1 <= nums.length <= 2 * 104
# -1000 <= nums[i] <= 1000
# -107 <= k <= 107
class Solution:
    def subarraySum(self, nums: List[int], k: int) -> int:
        # pre[i]记录前缀和 
        # 因此找到 pre[i] - pre[j] = k 即找到了符合条件的子串
        # 也就是 pre[i] - k = pre[j]的个数

        pre_dict = {0:1}
        pre = 0
        n = len(nums)
        res = 0
        for i in range(n):
            
            pre += nums[i] # 得到pre[i]
            res += pre_dict.get(pre - k, 0) #找到符合条件的 pre[i] - pre[j] = k
            pre_dict[pre] = pre_dict.get(pre, 0)  + 1 # 更新前缀和

            # print(pre_dict)
        return res




        # #暴力枚举
        # n = len(nums)
        # res = 0
        # for i in range(n):
        #     cur_sum = 0
        #     for j in range(i, -1, -1):
        #         cur_sum += nums[j]
        #         if cur_sum == k:
        #             res += 1
        # return res

            
