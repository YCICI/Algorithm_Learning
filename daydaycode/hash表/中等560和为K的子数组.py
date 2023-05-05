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
    def subarraySum(self, nums, k: int) -> int:
        # 字典记录字数组 这个不是递归吗？
        # 打印所有的子数组
         

        self.res = []
        self.n = 0
        def process(n, nums, path, K):
            if n > len(nums) - 1:
                if sum(path) == K:
                    self.n +=1
                self.res.append(path.copy())
                return
            
            # 当前  不要
            process(n + 1, nums, path, K)
            # 当前 要
            path.append(nums[n])
            process(n + 1, nums, path, K)
            path.pop()
            return
       
        process(0, nums, [], K)
        print(self.res)
        return self.n
    
f = Solution()
nums = [1,2,3]
K = 3
res = f.subarraySum(nums, K)
print(res)