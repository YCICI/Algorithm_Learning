# 148. 排序链表
# 给你链表的头结点 head ，请将其按 升序 排列并返回 排序后的链表 。

 

# 示例 1：


# 输入：head = [4,2,1,3]
# 输出：[1,2,3,4]
# 示例 2：


# 输入：head = [-1,5,3,4,0]
# 输出：[-1,0,3,4,5]
# 示例 3：

# 输入：head = []
# 输出：[]
 

# 提示：

# 链表中节点的数目在范围 [0, 5 * 104] 内
# -105 <= Node.val <= 105

# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def sortList(self, head: Optional[ListNode]) -> Optional[ListNode]:
        # 归并排序
        def get_mid(head):
            # print("get mid")
            if not head or not head.next:
                return head
            
            slow, fast = head, head.next
            while fast and fast.next:
                fast = fast.next.next
                slow = slow.next
            
            # 后半部分slow.next
            return slow
        
        def merge(list1, list2):
            if not list1 or not list2:
                return list1 if not list2 else list2
            dummy = ListNode(0)
            p = dummy
            p1, p2 = list1, list2
            while p1 and p2:
                if p1.val <= p2.val:
                    p.next = ListNode(p1.val)
                    p1 = p1.next
                    p = p.next
                
                elif p1.val > p2.val:
                    p.next = ListNode(p2.val)
                    p2 = p2.next
                    p = p.next
            
            p.next = p1 if p1 else p2
            return dummy.next
        
        def sortprocess(head):
            # print(head)
            if not head or not head.next:
                return head
            
            midnode = get_mid(head)
            # print("midnode", midnode)
            righthead = midnode.next
            midnode.next = None

            left = sortprocess(head)
            right = sortprocess(righthead)

            return merge(left, right)
        
        tmp = sortprocess(head)
        return tmp