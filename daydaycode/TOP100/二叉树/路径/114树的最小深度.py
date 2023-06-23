# 111. 二叉树的最小深度

# 给定一个二叉树，找出其最小深度。

# 最小深度是从根节点到最近叶子节点的最短路径上的节点数量。

# 说明：叶子节点是指没有子节点的节点。

 

# 示例 1：


# 输入：root = [3,9,20,null,null,15,7]
# 输出：2
# 示例 2：

# 输入：root = [2,null,3,null,4,null,5,null,6]
# 输出：5
 

# 提示：

# 树中节点数的范围在 [0, 105] 内
# -1000 <= Node.val <= 1000
# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def minDepth(self, root: Optional[TreeNode]) -> int:

        def process(root):
            if not root.left and not root.right:
                return 1
            min_depth = float('inf')
            if root.left:
                min_depth = min(process(root.left), min_depth)
            if root.right:
                min_depth = min(process(root.right), min_depth)

            return min_depth + 1
        
        if not root:
            return 0
        res = process(root)
        return res
    
# 104. 二叉树的最大深度


# 给定一个二叉树，找出其最大深度。

# 二叉树的深度为根节点到最远叶子节点的最长路径上的节点数。

# 说明: 叶子节点是指没有子节点的节点。

# 示例：
# 示例：
# 给定二叉树 [3,9,20,null,null,15,7]，

#     3
#    / \
#   9  20
#     /  \
#    15   7
# 返回它的最大深度 3 。
# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def maxDepth(self, root: Optional[TreeNode]) -> int:

        def process(root):
            if not root:
                return 0
            
            left = process(root.left)
            right = process(root.right)

            return max(left, right) + 1
        
        res = process(root)
        return res