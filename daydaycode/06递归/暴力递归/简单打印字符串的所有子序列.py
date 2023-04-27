# 题目四：打印一个字符串的全部子序列，包括空字符串
# 输出 "abc"
# 输出 ["abc", "ab", "ac", "a", "bc", "b", "c", ""]
### *********************  暴力递归 ********************* ### 
class Solution:
    def printall(self, str1):
        
        def process(i, str1, res):
            if i == len(str1):
                self.r_list.append(res)
                return
            
            # 要
            process(i+1, str1, res)
            # 不要
            process(i+1, str1, res + str1[i])
            return
        
        res = ""
        self.r_list = []
        process(0, str1, res)
        return self.r_list

S = Solution()
res = S.printall("abc")
print(res)