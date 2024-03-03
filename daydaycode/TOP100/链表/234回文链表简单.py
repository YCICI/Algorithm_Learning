# 234. 回文链表
# 已解答
# 简单
# 相关标签
# 相关企业
# 给你一个单链表的头节点 head ，请你判断该链表是否为回文链表。如果是，返回 true ；否则，返回 false 。

 

# 示例 1：


# 输入：head = [1,2,2,1]
# 输出：true
# 示例 2：


# 输入：head = [1,2]
# 输出：false
 

# 提示：

# 链表中节点数目在范围[1, 105] 内
# 0 <= Node.val <= 9
 

# 进阶：你能否用 O(n) 时间复杂度和 O(1) 空间复杂度解决此题？
# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def isPalindrome(self, head: Optional[ListNode]) -> bool:
        # 基础解法
        # arr = []
        # p1 = head

        # while p1:
        #     arr.append(p1.val)
        #     p1 = p1.next
        
        # return arr==arr[::-1]
        # 双指针 快慢指针 后半段翻转
        def end_of_first_half(head):
            fast = head
            slow = head
            while fast.next is not None and fast.next.next is not None:
                fast = fast.next.next
                slow = slow.next
            return slow

        def reverse_list(head):
            previous = None
            current = head
            while current is not None:
                next_node = current.next
                current.next = previous
                previous = current
                current = next_node
            return previous

        if not head:
            return True

        one_end = end_of_first_half(head)
        second_start = reverse_list(one_end.next)


        p1, p2 = head, second_start
        while p1 and p2:
            if p1.val != p2.val:
                return False
            p1 = p1.next
            p2 = p2.next
        return True

