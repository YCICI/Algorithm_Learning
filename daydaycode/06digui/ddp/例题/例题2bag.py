# 给定两个长度都为N的数组weights和values，weights[i〕和values[i]分别代表
# i号物品的重量和价值。给定一个正数bag，表示一个载重bag的袋子，你装的物
# 品不能超过这个重量。返回你能装下最多的价值是多少

#  ******************* base 暴力递归  ******************* #
def process(weights, values, i, alreadyvalues, alreadyWeight, bag):

    if alreadyWeight > bag:
        return 0 
    
    if i == len(values):
        return alreadyvalues
    
    return max(process(weights, values, i+ 1, alreadyvalues, alreadyWeight, bag) , 
               process(weights, values, i+ 1, alreadyvalues + values[i], alreadyWeight + weights[i] , bag))

def process2(weights, values, index, rest):

    if rest < 0 : ## base case 
        return -1 
    
    if index == len(values): 
        return 0
    
    # 不放当前物品
    p1 = process2(weights, values, index + 1, rest) 

    # 放当前物品
    p2 = -1
    p2next = process2(weights, values, index + 1, rest - weights[index]) 
    if p2next != -1:
        p2 = values[index] + p2next
    
    return max(p1, p2)

#  ******************* 改dp  ******************* #

def waydp(weights, values, rest, dp):
    
    n = len(weights)
    for i in range(n - 1, -1 , -1):
        for j in range(rest + 1):
            if i == n:
                dp[n][j] == 0
               # print(dp)

            
            p1 = dp[i + 1][j]
            p2 = -1
            if j - weights[i] >= 0:
                p2 = values[i] + dp[i + 1][j - weights[i]]
            dp[i][j] = max(p1, p2)
            #print(dp)
    print(dp)
    return dp[0][rest]


weights = [2, 1, 3, 2]
values = [1, 1, 4, 5]
bag = 5
rest = 5
res1 = process(weights, values, 0, 0, 0, bag)
print(res1)
res2 = process2(weights, values, 0, rest)
print(res2)

dp = [[0 for _ in range(bag + 1)] for _ in range(len(weights) + 1)]
print(dp)
res3 = waydp(weights, values, 5, dp)
print(res3)