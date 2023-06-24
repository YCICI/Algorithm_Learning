# 189. 轮转数组

# 给定一个整数数组 nums，将数组中的元素向右轮转 k 个位置，其中 k 是非负数。

 

# 示例 1:

# 输入: nums = [1,2,3,4,5,6,7], k = 3
# 输出: [5,6,7,1,2,3,4]
# 解释:
# 向右轮转 1 步: [7,1,2,3,4,5,6]
# 向右轮转 2 步: [6,7,1,2,3,4,5]
# 向右轮转 3 步: [5,6,7,1,2,3,4]
# 示例 2:

# 输入：nums = [-1,-100,3,99], k = 2
# 输出：[3,99,-1,-100]
# 解释: 
# 向右轮转 1 步: [99,-1,-100,3]
# 向右轮转 2 步: [3,99,-1,-100]
class Solution:
    def rotate(self, nums: List[int], k: int) -> None:
        """
        Do not return anything, modify nums in-place instead.
        """
class Solution:
    def rotate(self, nums: List[int], k: int) -> None:
        """
        Do not return anything, modify nums in-place instead.
        """

        def reverse_nums(nums, p1, p2):
            #print(arr)
            while p1 < p2:
                nums[p1], nums[p2] = nums[p2], nums[p1]
                p1 += 1
                p2 -= 1
            #print(arr)
            # return arr
        
        # 翻转数组
        n = len(nums)
        k %= n
        reverse_nums(nums, 0, n - 1) 
        reverse_nums(nums, 0, k - 1) ## 这里不能写reverse_nums(num[:k]) 这样的传参 相当于拷贝了一个数组 不会直接在原数组上翻转
        reverse_nums(nums, k, n - 1)




