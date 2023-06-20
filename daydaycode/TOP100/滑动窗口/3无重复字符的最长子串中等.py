# 3. 无重复字符的最长子串

# 给定一个字符串 s ，请你找出其中不含有重复字符的 最长子串 的长度。

 

# 示例 1:

# 输入: s = "abcabcbb"
# 输出: 3 
# 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。
# 示例 2:

# 输入: s = "bbbbb"
# 输出: 1
# 解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。
# 示例 3:

# 输入: s = "pwwkew"
# 输出: 3
# 解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。
#      请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。

from collections import deque
class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:
        # # 滑动窗口 暴力解法
        # n = len(s)
        # if n == 1:
        #     return 1
        # if n == 0:
        #     return 0
        # res = 1

        # for left in range(n):
        #     # print(left)
        #     if left > 0 and left < n - 1 and s[left] == s[left + 1]:
        #         continue
        #     for right in range(left + 1, n):
        #         # print(left, right)
        #         if s[right] not in s[left:right]:
        #             res = max(res, right - left + 1)
        #         else:
        #             break
                
                
        # return res
        ###### 滑动窗口 利用队列
        if not s:
            return 0
        
        queue = deque()
        n = len(s)
        res = 0

        for i in range(n):
            while s[i] in queue:
                queue.popleft()
            queue.append(s[i]) 
            res = max(res, len(queue))  

        return res
        

