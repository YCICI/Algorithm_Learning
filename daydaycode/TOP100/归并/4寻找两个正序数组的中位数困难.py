# 4. 寻找两个正序数组的中位数
# 已解答
# 困难
# 相关标签
# 相关企业
# 给定两个大小分别为 m 和 n 的正序（从小到大）数组 nums1 和 nums2。请你找出并返回这两个正序数组的 中位数 。

# 算法的时间复杂度应该为 O(log (m+n)) 。

 

# 示例 1：

# 输入：nums1 = [1,3], nums2 = [2]
# 输出：2.00000
# 解释：合并数组 = [1,2,3] ，中位数 2
# 示例 2：

# 输入：nums1 = [1,2], nums2 = [3,4]
# 输出：2.50000
# 解释：合并数组 = [1,2,3,4] ，中位数 (2 + 3) / 2 = 2.5
class Solution:
    def findMedianSortedArrays(self, nums1: List[int], nums2: List[int]) -> float:
        ## 寻找两个正序数组的中位数 找第k小的数

        def process(idx1, idx2, k):
            # print(idx1, idx2, k)
            if idx1 == n1 :
                return nums2[idx2 + k - 1]
            elif idx2 == n2 :
                return nums1[idx1 + k - 1]
            elif k == 1:
                return min(nums1[idx1], nums2[idx2])
            
            newidx1 = min(n1 - 1, idx1 + k // 2 - 1)
            newidx2 = min(n2 - 1, idx2 + k // 2  - 1)
            # print("new", newidx1, newidx2)
            if nums1[newidx1] <= nums2[newidx2]:
                return process(newidx1 + 1, idx2 , k - (newidx1 - idx1 + 1))
            
            else:
                return process(idx1 , newidx2 + 1, k - (newidx2 - idx2 + 1))
        
        n1, n2 = len(nums1), len(nums2)
        n = n1 + n2
        if n % 2 == 1:
            return process(0, 0, (n + 1) // 2)
        else:

            return (process(0, 0, n // 2) + process(0, 0, (n // 2) + 1)) / 2


