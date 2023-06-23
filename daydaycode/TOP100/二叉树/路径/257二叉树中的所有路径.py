# 257. 二叉树的所有路径



# 给你一个二叉树的根节点 root ，按 任意顺序 ，返回所有从根节点到叶子节点的路径。

# 叶子节点 是指没有子节点的节点。

 
# 示例 1：


# 输入：root = [1,2,3,null,5]
# 输出：["1->2->5","1->3"]
# 示例 2：

# 输入：root = [1]
# 输出：["1"]
# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def binaryTreePaths(self, root: Optional[TreeNode]) -> List[str]:
        # 暴力解 
        self.res = []
        
        def process(root, path):
            if not root.left and not root.right:
                self.res.append("->".join(path))
                return 
            # 
            if root.left:
                path.append(str(root.left.val))
                process(root.left, path)
                path.pop()
            if root.right:
                path.append(str(root.right.val))
                process(root.right, path)
                path.pop()
            return 
        
        process(root, [str(root.val)])
        return self.res
