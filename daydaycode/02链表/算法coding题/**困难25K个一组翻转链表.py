# 25. K 个一组翻转链表
# 给你链表的头节点 head ，每 k 个节点一组进行翻转，请你返回修改后的链表。
#
# k 是一个正整数，它的值小于或等于链表的长度。如果节点总数不是 k 的整数倍，那么请将最后剩余的节点保持原有顺序。
#
# 你不能只是单纯的改变节点内部的值，而是需要实际进行节点交换。
#
#
#
# 示例 1：
#
#
# 输入：head = [1,2,3,4,5], k = 2
# 输出：[2,1,4,3,5]
# 示例 2：
#
#
#
# 输入：head = [1,2,3,4,5], k = 3
# 输出：[3,2,1,4,5]
#
#
# 提示：
# 链表中的节点数目为 n
# 1 <= k <= n <= 5000
# 0 <= Node.val <= 1000
#
#
# 进阶：你可以设计一个只用 O(1) 额外内存空间的算法解决此问题吗？


# Definition for singly-linked list.
class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class Solution:
    def reverseKGroup(self, head: ListNode, k: int) -> ListNode:
        # 递归法
        # res_pre = ListNode(0)
        # res_pre.next = head
        # k_end_node = res_pre
        # for i in range(k):
        #     k_end_node = k_end_node.next
        #     if not k_end_node:
        #         return head

        # pre = head
        # cur = head.next
        # while pre != k_end_node:
        #     tmp = cur.next
        #     cur.next = pre
        #     pre = cur
        #     cur = tmp
        # head.next = self.reverseKGroup(cur,k)
        # return k_end_node

        # 迭代法
        res_pre = ListNode(0)
        res_pre.next = head

        pre_end = res_pre

        while pre_end.next:
            cur_start = pre_end.next
            cur_end = cur_start
            for i in range(k - 1):
                if not cur_end.next:
                    return res_pre.next
                    break
                cur_end = cur_end.next

            pre = None
            cur = cur_start
            while pre != cur_end:
                tmp = cur.next
                cur.next = pre
                pre = cur
                cur = tmp
            pre_end.next = cur_end  # 上一个未结点链接到翻转后的头节点上
            cur_start.next = cur  # 当前的尾结点链接到下一个的头结点上
            pre_end = cur_start  # 更新上一个尾结点为当前的的尾结点

        return res_pre.next