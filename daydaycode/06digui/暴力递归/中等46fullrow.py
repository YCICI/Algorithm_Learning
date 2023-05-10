# 46. 全排列
# 给定一个不含重复数字的数组 nums ，返回其 所有可能的全排列 。你可以 按任意顺序 返回答案。
#
#
#
# 示例 1：
#
# 输入：nums = [1,2,3]
# 输出：[[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]
# 示例 2：
#
# 输入：nums = [0,1]
# 输出：[[0,1],[1,0]]
# 示例 3：
#
# 输入：nums = [1]
# 输出：[[1]]
#
#
# 提示：
#
# 1 <= nums.length <= 6
# -10 <= nums[i] <= 10
# nums 中的所有整数 互不相同
class Solution:
    def permute(self, nums: [int]) -> [[int]]:

        def process(i, nums):
            # base
            if i == len(nums) :
                self.res.append(nums.copy())
                return 
            # 递归
            for j in range(i, len(nums)):
                # print("nums", nums)
                nums[i], nums[j] =nums[j], nums[i]
                process(i + 1, nums)
                nums[i], nums[j] = nums[j], nums[i]
            
            return
        
        self.res = []
        process(0, nums)
        return self.res
    
s = Solution()
nums = [0,1,2]
res = s.permute(nums)
print(res)
