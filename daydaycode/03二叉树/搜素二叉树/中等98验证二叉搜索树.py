# 98. 验证二叉搜索树
# 难度 
# 中等


# 给你一个二叉树的根节点 root ，判断其是否是一个有效的二叉搜索树。

# 有效 二叉搜索树定义如下：

# 节点的左子树只包含 小于 当前节点的数。
# 节点的右子树只包含 大于 当前节点的数。
# 所有左子树和右子树自身必须也是二叉搜索树。
 

# 示例 1：


# 输入：root = [2,1,3]
# 输出：true
# 示例 2：


# 输入：root = [5,1,4,null,null,3,6]
# 输出：false
# 解释：根节点的值是 5 ，但是右子节点的值是 4 。
 

# 提示：

# 树中节点数目范围在[1, 104] 内
# -231 <= Node.val <= 231 - 1

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def isValidBST(self, root: Optional[TreeNode]) -> bool:
        # 递归 - 中序遍历
        self.curvalue = float('-inf')

        def process(root):

            if not root:
                return True
            
            # 判断左子树
            left_valid = process(root.left)
            if not left_valid:
                return left_valid

            # 当前子树判断
            if root.val <= self.curvalue:
                return False
            else:
                self.curvalue = root.val
            
            # 判断右子树
            return process(root.right)

        return process(root)
    
# from collections import deque
# class Solution:
#     def isValidBST(self, root: Optional[TreeNode]) -> bool:
#         # 迭代 - 中序遍历
#         queue = deque()
#         cur = root
#         curvalue = float('-inf')

#         while cur or queue:
#             #
#             while cur:
#                 queue.append(cur)
#                 cur = cur.left
            
#             # print(queue)
#             cur = queue.pop()
#             if cur.val <= curvalue:
#                 return False
#             else:
#                 curvalue = cur.val
#             cur = cur.right
#         return True