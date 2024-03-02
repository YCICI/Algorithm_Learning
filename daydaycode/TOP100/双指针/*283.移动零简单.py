
# 283. 移动零

# 给定一个数组 nums，编写一个函数将所有 0 移动到数组的末尾，同时保持非零元素的相对顺序。

# 请注意 ，必须在不复制数组的情况下原地对数组进行操作。

# 示例 1:

# 输入: nums = [0,1,0,3,12]
# 输出: [1,3,12,0,0]
# 示例 2:

# 输入: nums = [0]
# 输出: [0]
class Solution:
    def moveZeroes(self, nums: List[int]) -> None:
        """
        Do not return anything, modify nums in-place instead.
        """
        # 快排 以0为界限 不等于0的放右边
        p1, p2 = 0, 0
        lent = len(nums) 
        while p2 < lent:
            if nums[p2] :
                nums[p1], nums[p2] = nums[p2], nums[p1]
                p1 += 1
            p2 +=1