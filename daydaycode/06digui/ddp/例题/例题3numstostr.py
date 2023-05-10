# 6.数字串转成字符串
# 规定1和A对应、2和B对应、3和C对应..
# 那么一个数字字符串比如"111"，就可以转化为"AAA"、"KA"和"AK”。
# 给定一个只有数字字符组成的字符串str，返回有多少种转化结果


# 0开头
# 1开头
# 2开头，第二个数大于6，和小于6
# 大于3开头
class Solution:
    def numbertostr(self, str):
        # ***************** 暴力解法 ***************** #
        def process(str, i):

            if i == len(str):
                #print(i)
                self.res += 1
                return 1
            
            if str[i] == '0':
                return -1
            
            if str[i] == '1':
                if i + 2 <= len(str):
                    return process(str, i + 1) + process(str, i + 2) 
                return  process(str, i + 1) 
            if str[i] =='2':
                if i + 2 <= len(str) and str[i + 1] <= '6':
                    return process(str, i + 1) + process(str, i + 2) 
                return process(str, i + 1) 
   
            return  process(str, i + 1)
        
        self.res = 0
        process(str, 0)
        return self.res


def waydp(str1, dp):

    dp[len(str1)] = 1
    
    for i in range(len(str1) - 1, -1, -1):
        print("i", i)
        if str1[i] == '1':
            print(" = 1")
            if i + 2 <= len(str1) + 1:
                dp[i] = dp[i + 1] + dp[i + 2]
        dp[i] = dp[i + 1]

        if str1[i] =='2':
            if i + 2 <= len(str1) and str1[i + 1] <= '6':
                dp[i] = dp[i + 1] + dp[i + 2]
        dp[i] = dp[i + 1]
    
    print(dp)
    return  dp[0]
        

    
f = Solution()
str1 = '111'
res = f.numbertostr(str1)
print(res)

dp = [0 for _ in range(len(str1) + 1)]
res2 = waydp(str1, dp)
print(res2)
