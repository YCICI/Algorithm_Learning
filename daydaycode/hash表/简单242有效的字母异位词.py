# 242. 有效的字母异位词
# 给定两个字符串 s 和 t ，编写一个函数来判断 t 是否是 s 的字母异位词。
#
# 注意：若 s 和 t 中每个字符出现的次数都相同，则称 s 和 t 互为字母异位词。
#
#
#
# 示例 1:
#
# 输入: s = "anagram", t = "nagaram"
# 输出: true
# 示例 2:
#
# 输入: s = "rat", t = "car"
# 输出: false
#
#
# 提示:
#
# 1 <= s.length, t.length <= 5 * 104
# s 和 t 仅包含小写字母
#
#
# 进阶: 如果输入字符串包含 unicode 字符怎么办？你能否调整你的解法来应对这种情况？


class Solution:
    #  ************ 解法一： ************ #  
    def isAnagram1(self, s: str, t: str) -> bool:

        
        record = [0] * 26

        for i in range(len(s)):
            record[ord(s[i]) - ord('a')] +=1
        for j in range(len(t)):
            inx = ord(t[j]) - ord('a')
            if not record[inx]:
                return False
            record[inx] -=1
        
        return sum(record) == 0
    
    #  ************ 解法二 ************ #  
    def isAnagram2(self, s: str, t: str) -> bool:
        #
        record = {}

        for i in s:
            record[i] = record.get(i, 0) + 1 
        
        # print(record)
        for j in t:
            if j not in record:
                return False
            record[j] = record.get(j, 0) - 1 
        
        return sum(record.values()) == 0

f = Solution()
s = "anagram"
t = "nagaram"
res1 = f.isAnagram1(s, t)
print(res1)

res2 = f.isAnagram2(s, t)
print(res2)
    

