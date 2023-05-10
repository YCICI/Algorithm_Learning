# 3. 无重复字符的最长子串
# 给定一个字符串 s ，请你找出其中不含有重复字符的 最长子串 的长度。
#
#
#
# 示例 1:
#
# 输入: s = "abcabcbb"
# 输出: 3
# 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。
# 示例 2:
#
# 输入: s = "bbbbb"
# 输出: 1
# 解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。
# 示例 3:
#
# 输入: s = "pwwkew"
# 输出: 3
# 解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。
# 请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。
# 示例 4:
#
# 输入: s = ""
# 输出: 0
#
#
# 提示：
#
# 0 <= s.length <= 5 * 104
# s 由英文字母、数字、符号和空格组成

class Solution():
    def get_max_different_substr(self, s):

        n = len(s)
        s_map = {}
        start_idx = 0
        res = 0

        for idx in range(n):
            # 更新字符串开头index
            if s[idx] in s_map:
                start_idx = s_map[s[idx]]
            
            #更新子串长度
            else:
                length = idx - start_idx
                res = max(length, res)
            
            s_map[s[idx]] = idx
        
        return res
    
f = Solution()
s = "pwwkew"
res = f.get_max_different_substr(s)     
print(res)