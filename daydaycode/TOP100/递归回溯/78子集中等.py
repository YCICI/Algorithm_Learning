# 78. 子集
# 已解答
# 中等
# 相关标签
# 相关企业
# 给你一个整数数组 nums ，数组中的元素 互不相同 。返回该数组所有可能的子集（幂集）。

# 解集 不能 包含重复的子集。你可以按 任意顺序 返回解集。

 

# 示例 1：

# 输入：nums = [1,2,3]
# 输出：[[],[1],[2],[1,2],[3],[1,3],[2,3],[1,2,3]]
# 示例 2：

# 输入：nums = [0]
# 输出：[[],[0]]
 

# 提示：

# 1 <= nums.length <= 10
# -10 <= nums[i] <= 10
# nums 中的所有元素 互不相同
class Solution:
    def subsets(self, nums: List[int]) -> List[List[int]]:
        
        self.res = []
        n = len(nums)

        def process(idx, path):
            if idx == n :
                self.res.append(path[:])
                return 
            
            # idx 什么都不选
            process(idx + 1, path)
            
            # idx 选择这个位置上的数
            path.append(nums[idx])
            process(idx + 1, path)
            path.pop()
            return
        
        process(0, [])
        return self.res