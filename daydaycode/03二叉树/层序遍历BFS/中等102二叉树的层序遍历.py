
# 102. 二叉树的层序遍历
# 难度
# 中等
# 给你二叉树的根节点 root ，返回其节点值的 层序遍历 。 （即逐层地，从左到右访问所有节点）。

# 示例 1：

# 输入：root = [3,9,20,null,null,15,7]
# 输出：[[3],[9,20],[15,7]]
# 示例 2：

# 输入：root = [1]
# 输出：[[1]]
# 示例 3：

# 输入：root = []
# 输出：[]
 

# 提示：
# 树中节点数目在范围 [0, 2000] 内
# -1000 <= Node.val <= 1000

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right


# ----------------------------------------解法1 递归  ----------------------------------------
from collections import deque
class Solution:
    def levelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
        # 递归方法
        def process(index, root):

            if not root:
                return

            if len(res) < index:
                res.append([])
            
            res[index - 1].append(root.val)
            process(index + 1, root.left)
            process(index + 1, root.right)
            return 

        res = []
        process(1, root)
        return res
    
# ----------------------------------------解法2 迭代 记录当前所在层----------------------------------------
# # Definition for a binary tree node.
# # class TreeNode:
# #     def __init__(self, val=0, left=None, right=None):
# #         self.val = val
# #         self.left = left
# #         self.right = right
# from collections import deque
# class Solution:
#     def levelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
        # 迭代
        # if not root:
        #     return []
    
        # res, queue = [], deque()
        
        # queue.append(root)
        # help = {root: 0}

        # while queue:
        #     cur = queue.popleft()
        #     level = help[cur]
        #     if len(res) - 1 < level:
        #         res.append([])

        #     res[level].append(cur.val)
        #     if cur.left:
        #         queue.append(cur.left)
        #         help[cur.left] = (level + 1)
        #     if cur.right:
        #         queue.append(cur.right)
        #         help[cur.right] = (level + 1)

        # return res

# ----------------------------------------解法2 迭代2 记录层的节点数 空间复杂度更优----------------------------------------
# from collections import deque
# class Solution:
#     def levelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
#         # 迭代
#         if not root:
#             return []
        
#         res, level, queue= [], 0, deque()
#         queue.append(root)

#         while queue:
            
#             size = len(queue)
#             help = []

#             for _ in range(size):
#                 cur = queue.popleft()
#                 help.append(cur.val)

#                 if cur.left:
#                     queue.append(cur.left)
#                 if cur.right:
#                     queue.append(cur.right)
            
#             res.append(help)
        
#         return res






        
        

        
        
