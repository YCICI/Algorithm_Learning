# 5. 最长回文子串
# 给你一个字符串 s，找到 s 中最长的回文子串。
#
#
#
# 示例 1：
#
# 输入：s = "babad"
# 输出："bab"
# 解释："aba" 同样是符合题意的答案。
# 示例 2：
#
# 输入：s = "cbbd"
# 输出："bb"
# 示例 3：
#
# 输入：s = "a"
# 输出："a"
# 示例 4：
# 
# 输入：s = "ac"
# 输出："a"
#
#
# 提示：
#
# 1 <= s.length <= 1000
# s 仅由数字和英文字母（大写和/或小写）组成
class Solution():
    # ************* 暴力解法  ************* #
    def get_max_substr_with_center_expand1(self, s):

        res = ''
        n = len(s)
        max_len = 0

        for idx in range(n):
            left_index = idx
            right_index = idx + 1
            cur_len = 0
            while left_index > 0 and right_index < n:
                if s[left_index] == s[right_index]:
                    cur_s = s[left_index : right_index + 1]
                    cur_len = right_index - left_index + 1
                left_index -=1
                right_index +=1
            
            if cur_len > max_len:
                res = cur_s
                max_len = cur_len

        return res
    
f = Solution()
s = "babad"
res = f.get_max_substr_with_center_expand1(s)
print(res)
