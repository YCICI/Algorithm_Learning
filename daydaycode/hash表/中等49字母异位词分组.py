# 49. 字母异位词分组


# 给你一个字符串数组，请你将 字母异位词 组合在一起。可以按任意顺序返回结果列表。

# 字母异位词 是由重新排列源单词的字母得到的一个新单词，所有源单词中的字母通常恰好只用一次。

 

# 示例 1:

# 输入: strs = ["eat", "tea", "tan", "ate", "nat", "bat"]
# 输出: [["bat"],["nat","tan"],["ate","eat","tea"]]
# 示例 2:

# 输入: strs = [""]
# 输出: [[""]]
# 示例 3:

# 输入: strs = ["a"]
# 输出: [["a"]]

# **************** hash **************** #
class Solution:
    def groupAnagrams(self, nums, target):
        # 
        sort_strs = ["".join(sorted(s)) for s in nums]
        res_dict = {}

        for i in range(len(sort_strs)):
            tmp = res_dict.get(sort_strs[i], [])
            tmp.append(nums[i])
            res_dict[sort_strs[i]] = tmp


