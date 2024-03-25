# 23. 合并 K 个升序链表
# 已解答
# 困难
# 相关标签
# 相关企业
# 给你一个链表数组，每个链表都已经按升序排列。

# 请你将所有链表合并到一个升序链表中，返回合并后的链表。

 

# 示例 1：

# 输入：lists = [[1,4,5],[1,3,4],[2,6]]
# 输出：[1,1,2,3,4,4,5,6]
# 解释：链表数组如下：
# [
#   1->4->5,
#   1->3->4,
#   2->6
# ]
# 将它们合并到一个有序链表中得到。
# 1->1->2->3->4->4->5->6
# 示例 2：

# 输入：lists = []
# 输出：[]
# 示例 3：

# 输入：lists = [[]]
# 输出：[]
 

# 提示：

# k == lists.length
# 0 <= k <= 10^4
# 0 <= lists[i].length <= 500
# -10^4 <= lists[i][j] <= 10^4
# lists[i] 按 升序 排列
# lists[i].length 的总和不超过 10^4
# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
# class Solution:
#     def mergeKLists(self, lists: List[Optional[ListNode]]) -> Optional[ListNode]:
#         # 解法一 归并 从中间分开 左边合并 右边合并
#         # 解法二 维护一个长度为k的优选队列 弹出最小值后 更新该值的next进入优先队列

#         def mergetwolists(list1, list2):
#             if not list1 or not list2:
#                 return list1 if not list2 else list2
#             dummy = ListNode(0)
#             p = dummy
#             p1, p2 = list1, list2
#             while p1 and p2:
#                 if p1.val <= p2.val:
#                     p.next = ListNode(p1.val)
#                     p1 = p1.next
#                     p = p.next
                
#                 elif p1.val > p2.val:
#                     p.next = ListNode(p2.val)
#                     p2 = p2.next
#                     p = p.next
            
#             p.next = p1 if p1 else p2
#             return dummy.next

#         def mergeprocess(left, right):
#             # print(left, right)
#             if right <= left:
#                 return lists[right]
            
#             if right - left == 1:
#                 return mergetwolists(lists[left], lists[right])

#             mid = left + ((right - left) // 2)
#             left = mergeprocess(left, mid - 1)
#             right = mergeprocess(mid, right)

#             return mergetwolists(left, right)
        
#         if not lists:
#             return 
#         n = len(lists)
#         return mergeprocess(0, n-1)
# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
ListNode.__lt__ = lambda a, b: a.val < b.val 
class Solution:
    def mergeKLists(self, lists: List[Optional[ListNode]]) -> Optional[ListNode]:
        # 解法二 堆 
        # 把链表的指针第一项都加入堆 依次弹出最小值 
        # 更新最小值的的next进入堆
        if not lists :
            return 

        n = len(lists)
        pq = [(lists[i].val, lists[i]) for i in range(n) if lists[i]]
        heapq.heapify(pq)

        dummy = ListNode(-1)
        p = dummy
        while pq:
            min_val, min_node = heapq.heappop(pq)
            p.next = ListNode(min_val)
            p = p.next
            if min_node.next:
                heapq.heappush(pq, (min_node.next.val, min_node.next))
            
        return dummy.next