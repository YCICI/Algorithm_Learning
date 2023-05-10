# 给定一个不含重复字母的数组 nums ，返回其 所有可能的全排列 。你可以 按任意顺序 返回答案。
#
# 示例 1：
#
# 输入：str = 'abc'
# 输出：['abc','acb','bac','bca','cab','cba']
# 示例 2：
#
# 输入：nums = 'ab'
# 输出：['ab','ba']
# 示例 3：
#
# 输入：nums = 'a'
# 输出：['a']
#
#
# 提示：
#
# 1 <= nums.length <= 6
# -10 <= nums[i] <= 10
# nums 中的所有整数 互不相同
class Solution:
    def permute(self, str: [int]) -> [[int]]:
        
       
        def process(i, str_list):
            
            # base
            if i == len(str_list):
                self.res.append("".join(str_list))
                return
            
            # 递归
            for j in range(i, len(str_list)):
                str_list[i], str_list[j] = str_list[j], str_list[i]
                process(i + 1, str_list)
                str_list[i], str_list[j] = str_list[j], str_list[i]
            
            return
        
        self.res = []
        process(0 , list(str))
        
        return self.res 

s = Solution()
res = s.permute('ab')
print(res)
