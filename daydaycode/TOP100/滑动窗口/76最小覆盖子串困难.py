# 76. 最小覆盖子串
# 已解答
# 困难
# 相关标签
# 相关企业
# 提示
# 给你一个字符串 s 、一个字符串 t 。返回 s 中涵盖 t 所有字符的最小子串。如果 s 中不存在涵盖 t 所有字符的子串，则返回空字符串 "" 。

 

# 注意：

# 对于 t 中重复字符，我们寻找的子字符串中该字符数量必须不少于 t 中该字符数量。
# 如果 s 中存在这样的子串，我们保证它是唯一的答案。
 

# 示例 1：

# 输入：s = "ADOBECODEBANC", t = "ABC"
# 输出："BANC"
# 解释：最小覆盖子串 "BANC" 包含来自字符串 t 的 'A'、'B' 和 'C'。
# 示例 2：

# 输入：s = "a", t = "a"
# 输出："a"
# 解释：整个字符串 s 是最小覆盖子串。
# 示例 3:

# 输入: s = "a", t = "aa"
# 输出: ""
# 解释: t 中两个字符 'a' 均应包含在 s 的子串中，
# 因此没有符合条件的子字符串，返回空字符串。
 

# 提示：

# m == s.length
# n == t.length
# 1 <= m, n <= 105
# s 和 t 由英文字母组成
class Solution:
    def minWindow(self, s: str, t: str) -> str:
        # # 
        need=collections.defaultdict(int)
        if not s or not t or len(s) < len(t):
            return ""
        
        # 获得need
        res = [0, float("inf")]
        for c in t:
            need[c]+=1
        needcnt = len(t)

        # 滑动窗口遍历
        left = 0
        for right, cur_s in enumerate(s):

            # 右移
            if need[cur_s] > 0:
                # 只有needcnt计数过的数neen才会大于0，多余的数字need值都是小于1的
                needcnt -= 1
            need[cur_s] -= 1
            
            
            if needcnt == 0:
                
                # 左移去除多余元素
                while True:
                    left_s = s[left] 
                    if need[left_s]==0:
                        # ==0 表示左移到了关键字符
                        break
                    need[left_s] += 1
                    left += 1

                if right - left < res[1] - res[0]:
                    res = [left, right]
                #
                need[s[left]]+=1 
                left += 1
                needcnt += 1

                
        return '' if res[1]>len(s) else s[res[0]:res[1]+1]
                    

        # need=collections.defaultdict(int)
        # if not s or not t or len(s) < len(t):
        #     return ''

        # ## 按照t更新需要的字符串
        # for c in t:
        #     need[c]+=1
        # needCnt=len(t)
        # ## need[c] 有三个状态=1表示需要1个，=0表示不需要，小于0表示含有多个

        # ## 滑动窗口开始更新
        # left = 0
        # res=(0,float('inf'))
        # for right, cur_s in enumerate(s):
        #     # 右移 直到needCnt == 0
        #     if need[cur_s]>0:
        #         needCnt -=1
        #     need[cur_s]-=1

        #     # 当前滑动窗口包含所有元素 
        #     if needCnt==0:   
        #         # 开始左移 去除多余元素    
        #         while True:      
        #             left_s = s[left] 
        #             if need[left_s]==0:
        #                 break
        #             need[left_s] +=1
        #             left +=1
        #         # 更新结果
        #         if right - left < res[1]-res[0]:  
        #             res=(left,right)
                
        #         # 更新一位 继续
        #         need[s[left]]+=1 
        #         needCnt+=1
        #         left+=1
        # return '' if res[1]>len(s) else s[res[0]:res[1]+1]    #如果res始终没被更新过，代表无满足条件的结果
