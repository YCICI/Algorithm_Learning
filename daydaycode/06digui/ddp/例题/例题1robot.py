# ## 题目一：机器人找位置
# * 递归解法
# * 递归改基本dp
# * 进一步优化dp


# ### 题目一：机器人找位置
# 假设有排成一行的N个位置记为1~N，N一定大于或等于2    
# 开始时机器人在其中的M位置上(M一定是1~N中的一个) 
# 如果机器人来到1位置，那么下一步只能往右来到2位置；  
# 如果机器人来到N位置，那么下一步只能往左来到N-1位置；    
# 如果机器人来到中间位置，那么下一步可以往左走或者往右走；    
# 规定机器人必须走K步，最终能来到P位置(P也是1~N中的一个)的方法有多少种    
# 给定四个参数 N、M、K、P，返回方法数


# ***************** base 递归解法  *****************#
def process(N, cur, have, P ):
    
    # base case
    if have == 0:
        return  1 if cur == P else 0
    if cur == 1:
        return process(N, cur + 1, have - 1, P )
    if cur == N:
        return process(N, cur - 1, have - 1, P )
    
    #
    return process(N, cur + 1, have - 1, P ) + process(N, cur - 1, have - 1, P )

# ***************** base 动态规划  *****************#
# 返回之前把结果记录缓存下来
def waydp(N, cur, rest, P, dp):
    
    # base case
    dp[P][0] = 1
    # print(dp)

    for i in range(1, rest + 1):
        for j in range(1, N):
            if j == 1:
                dp[j][i] = dp[j + 1][i - 1]
            if j == N:
                dp[j][i]  = dp[j - 1][i - 1]
            
            #print(j, i)
            dp[j][i]  = dp[j + 1][i - 1]  + dp[j - 1][i - 1] 
        
        # print(dp)
    
    return dp[cur][rest]

N = 7
M = 3
K = 3
P = 2

dp = [[0 for _ in range(K + 1)] for _ in range(N + 1)]  # dp[i][j]表示
process(N, M, K, P)
