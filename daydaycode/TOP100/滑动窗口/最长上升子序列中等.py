# 最长上升子序列(三)
# 题目
# 题解(76)
# 讨论(118)
# 排行
# 面经new
#  时间限制：1秒  空间限制：256M
# 知识点
# 二分
# 动态规划
# 描述
# 给定数组 arr ，设长度为 n ，输出 arr 的最长上升子序列。（如果有多个答案，请输出其中 按数值(注：区别于按单个字符的ASCII码值)进行比较的 字典序最小的那个）

# 数据范围：0 \le n \le 200000 , 0 \le arr_i \le 10000000000≤n≤200000,0≤arr 
# i
# ​
#  ≤1000000000
# 要求：空间复杂度 O(n)O(n)，时间复杂度 O(nlogn)O(nlogn)
# 示例1
# 输入：
# [2,1,5,3,6,4,8,9,7]
# 复制
# 返回值：
# [1,3,4,8,9]
# 复制
# 示例2
# 输入：
# [1,2,8,6,4]
# 复制
# 返回值：
# [1,2,4]
# 复制
# 说明：
# 其最长递增子序列有3个，（1，2，8）、（1，2，6）、（1，2，4）其中第三个 按数值进行比较的字典序 最小，故答案为（1，2，4）         
# 牛客网链接 https://www.nowcoder.com/practice/9cf027bf54714ad889d4f30ff0ae5481


arr = [2,1,5,3,6,4,8,9,7]

#  解题思路 首先按照基础的最长上升子序列解题 返回基础的子序列长度
#  但是实际更新子序列的时候 破坏了原有子序列的顺序 例如例1 最后的结果是[1,3,7,8,9] 虽然长度没有问题但是不是真正的子序列 因此需要对结果进行修正
#  维护一个数组 max_len 记录以i位置结尾最长子序列的长度 例如例1就是 [2,1,5,3,6,4,8,9,7] max_len = [1,1,2,2,3,3,4,5,4] 
#  倒序遍历 取最先出现的max_len[i] = len 的值  保证子序列的原有顺序
stack = []
n = len(arr)
max_len = [1] * n
res = []

for i in range(n):
    if stack and arr[i] <= stack[-1]:
        left, right = 0, len(stack)
        while left <= right:
            mid = left + ((right - left) // 2)
            target = arr[i]
            if stack[mid] >= target:
                right = mid - 1
                ans = mid
            else:
                left = mid + 1
        stack[ans] = target
        max_len[i] = ans + 1
    else:
        stack.append(arr[i])
        max_len[i] = len(stack)
    
    # cur_len = len(stack)
    # max_len[i] = cur_len

# print(stack)
# print(arr)
# print(max_len)
len_s = len(stack)
for j in range(len(max_len) - 1, -1, -1):
    if max_len[j] == len_s:
        stack[len_s - 1] = arr[j]
        len_s -= 1

print(stack)
    
    