# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
from collections import deque
class Solution:
    def isValidBST(self, root: Optional[TreeNode]) -> bool:
        # 树型DP  解法
        # 判断左右子树分别提供什么信息
        # 判断当前树是否满足条件

        res = self.process(root)
        return res[0]

    def process(self, root):

        if not root:
            return 
 
        left_data = self.process(root.left)
        right_data = self.process(root.right)
        
        max_value = root.val
        min_value = root.val

        if left_data :
            max_value = max(max_value, left_data[1] )
            min_value = min(min_value, left_data[2] )

        if right_data:
            max_value = max(max_value, right_data[1] )
            min_value = min(min_value, right_data[2] )

        isbst = True
        if left_data and (not left_data[0] or left_data[1] >= root.val ):
            isbst = False

        if right_data and (not right_data[0] or right_data[2] <= root.val):
            isbst = False

        return (isbst, max_value, min_value)

