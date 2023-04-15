# 110. 平衡二叉树


# 给定一个二叉树，判断它是否是高度平衡的二叉树。

# 本题中，一棵高度平衡二叉树定义为：

# 一个二叉树每个节点 的左右两个子树的高度差的绝对值不超过 1 。

 

# 示例 1：


# 输入：root = [3,9,20,null,null,15,7]
# 输出：true
# 示例 2：


# 输入：root = [1,2,2,3,3,null,null,4,4]
# 输出：false
# 示例 3：

# 输入：root = []
# 输出：true
 

# 提示：

# 树中的节点数在范围 [0, 5000] 内
# -104 <= Node.val <= 104

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def isBalanced(self, root: Optional[TreeNode]) -> bool:
        # 递归 树型DP
        res = self.process(root)
        return res[0]

    def process(self, root):
        if not root:
            return (True, 0)

        left_isb, leftheight = self.process(root.left)
        right_isb, rightheight = self.process(root.right)

        isb = False
        
        if left_isb and right_isb and abs( leftheight - rightheight) <= 1 :
            isb = True

        return (isb, max(leftheight, rightheight) + 1 )