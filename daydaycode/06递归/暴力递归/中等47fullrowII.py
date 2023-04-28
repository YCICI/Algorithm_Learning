# 46. 全排列
# 给定一个含重复数字的数组 nums ，返回其 所有可能的全排列 。你可以 按任意顺序 返回答案。去除重复的数字排列
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

        def process(i, nums, used):
            # print("i", i)

            # base
            if i == len(nums) :
               # print("res", nums)
                self.res.append(nums.copy())
                return 
            
            # 递归
            used = []
            for j in range(i, len(nums)):
                # print("nums", nums[j])
                # print("used", used)
                if nums[j] not in used:
                    used.append(nums[j])
                    nums[i], nums[j] =nums[j], nums[i]
                    process(i + 1, nums, used)
                    nums[i], nums[j] = nums[j], nums[i]
                
            return
        
        self.res = []
        used = [] 
        process(0, nums, used)
        return self.res
    
s = Solution()
nums = [0, 1, 1]
res = s.permute(nums)
print(res)
