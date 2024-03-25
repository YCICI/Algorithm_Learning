# 236. 二叉树的最近公共祖先
# 已解答
# 中等
# 相关标签
# 相关企业
# 给定一个二叉树, 找到该树中两个指定节点的最近公共祖先。

# 百度百科中最近公共祖先的定义为：“对于有根树 T 的两个节点 p、q，最近公共祖先表示为一个节点 x，满足 x 是 p、q 的祖先且 x 的深度尽可能大（一个节点也可以是它自己的祖先）。”

 

# 示例 1：


# 输入：root = [3,5,1,6,2,0,8,null,null,7,4], p = 5, q = 1
# 输出：3
# 解释：节点 5 和节点 1 的最近公共祖先是节点 3 。
# 示例 2：


# 输入：root = [3,5,1,6,2,0,8,null,null,7,4], p = 5, q = 4
# 输出：5
# 解释：节点 5 和节点 4 的最近公共祖先是节点 5 。因为根据定义最近公共祖先节点可以为节点本身。
# 示例 3：

# 输入：root = [1,2], p = 1, q = 2
# 输出：1
 

# 提示：

# 树中节点数目在范围 [2, 105] 内。
# -109 <= Node.val <= 109
# 所有 Node.val 互不相同 。
# p != q
# p 和 q 均存在于给定的二叉树中。
# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, x):
#         self.val = x
#         self.left = None
#         self.right = None

class Solution:
    def lowestCommonAncestor(self, root: 'TreeNode', p: 'TreeNode', q: 'TreeNode') -> 'TreeNode':
        
        # 方法一 递归
        # def process(root, p, q):
        #     # print(root,'\n')
        #     if not root:
        #         return False
        
        #     left = process(root.left, p, q)
        #     right = process(root.right, p, q)
            
        #     if ((left and right) or ((root.val == p.val or root.val == q.val) and (left or right))):
        #         self.res = root
        #         ## 为什么这里一定是深度 当最深的公共祖先x遍历到后 fx被设定为true 
        #         ## 就是假定个子树中只有一个 p 节点或 q 节点，因此其他公共祖先不会再被判断为符合条件。

        #     return left or right or (root.val == p.val or root.val == q.val)
        
        # self.res = None
        # process(root, p, q)
        # return self.res
        # 方法二 记录父节点
