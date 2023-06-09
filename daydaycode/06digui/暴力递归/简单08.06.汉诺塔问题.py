# 在经典汉诺塔问题中，有 3 根柱子及 N 个不同大小的穿孔圆盘，盘子可以滑入任意一根柱子。一开始，所有盘子自上而下按升序依次套在第一根柱子上(即每一个盘子只能放在更大的盘子上面)。移动圆盘时受到以下限制:
# (1) 每次只能移动一个盘子;
# (2) 盘子只能从柱子顶端滑出移到下一根柱子;
# (3) 盘子只能叠在比它大的盘子上。

# 请编写程序，用栈将所有盘子从第一根柱子移到最后一根柱子。

# 你需要原地修改栈。

# 示例1:

#  输入：A = [2, 1, 0], B = [], C = []
#  输出：C = [2, 1, 0]
# 示例2:

#  输入：A = [1, 0], B = [], C = []
#  输出：C = [1, 0]
### *********************  暴力递归 ********************* ### 
class Solution:
    def hanota(self, A, B, C) -> None:
        """
        Do not return anything, modify C in-place instead.
        """
        # A-from B-others C-to
        # step1 0 ~ i-1 from 移到 others
        # step2 i 移到 to
        # step3 0 ～ i-1 others 移到 to

        def process(N, f, to, others):
            # print("N", N)
            if N == 1:
                num = f.pop()
                to.append(num)
                return
            
            process(N-1, f, others, to)
            num = f.pop()
            to.append(num)
            process(N-1, others, to, f)

            return
        
        N = len(A)
        process(N, A, C, B)
        return C

if __name__ == "__main__": 
    A = [2, 1, 0]
    B = []
    C = []
    s = Solution()
    res = s.hanota(A, B, C)
    print(res)
    



        