# 22. 括号生成
# 数字 n 代表生成括号的对数，请你设计一个函数，用于能够生成所有可能的并且 有效的 括号组合。
#
#
#
# 示例 1：
#
# 输入：n = 3
# 输出：["((()))","(()())","(())()","()(())","()()()"]
# 示例 2：
#
# 输入：n = 1
# 输出：["()"]
#
#
# 提示：
#
# 1 <= n <= 8

class Solution:
    def generateParenthesis(self, n: int) -> List[str]:


        def process(cur_str, n, left_used, righ_used):

            if n == left_used and n == righ_used:
                self.res.append(cur_str)
                return 
            
            if left_used <= n:
                process(cur_str + '(', left_used + 1, righ_used)
            
            if righ_used <= n:
                process(cur_str + ')', left_used, righ_used + 1)


            return
        
        self.res = []
        process("", n, 0, 0)

        return 