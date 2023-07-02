# 假设你正在爬楼梯。需要 n 阶你才能到达楼顶。
# 每次你可以爬 1 或 2 或 3 个台阶。你有多少种不同的方法可以爬到楼顶呢？


# 
def solution(n):
    #
    if n <=2 :
        return n
    if n == 3:
        return 4
    
    #
    dp = [0] * n
    # 
    dp[1] = 1
    dp[2] = 2
    dp[3] = 4 
    # print(dp)
    
    # 
    for i in range(4, n):
        # print("i", i)
        dp[i] = dp[i - 1] + dp[i - 2] + dp[i - 3] 
    
    # print(dp)
    return dp[n - 1]


n = 11
res = solution(n)
print(res)

