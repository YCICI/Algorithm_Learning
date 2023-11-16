# 92. 反转链表 II
# 中等
# 1.7K
# 相关企业
# 给你单链表的头指针 head 和两个整数 left 和 right ，其中 left <= right 。请你反转从位置 left 到位置 right 的链表节点，返回 反转后的链表 。
 

# 示例 1：


# 输入：head = [1,2,3,4,5], left = 2, right = 4
# 输出：[1,4,3,2,5]
# 示例 2：

# 输入：head = [5], left = 1, right = 1
# 输出：[5]
 

# 提示：

# 链表中节点数目为 n
# 1 <= n <= 500
# -500 <= Node.val <= 500
# 1 <= left <= right <= n
class Solution:
    def reverseBetween(self, head: Optional[ListNode], left: int, right: int) -> Optional[ListNode]:
        ### ------------------- 基础解法  ------------------------
        # def reverse_listnode(l1):
        #     # 反转链表
        #     pre, cur = None, l1
        #     while cur:
        #         tmp = cur.next
        #         cur.next = pre
        #         pre = cur
        #         cur = tmp

        # # 把链表分成三段 pre 需要反转的部分 right
        # dummy_node = ListNode(-1)
        # dummy_node.next = head
        # pre = dummy_node
        
        # # 找到要截断的节点
        # for _ in range(left - 1):
        #     pre = pre.next
        # # print("pre", pre, )

        # #
        # right_node = pre
        # for _ in range(right - left + 1):
        #     right_node = right_node.next
        # # print("right_node", right_node)
        # #
        # reverse_node = pre.next
        # curr = right_node.next

        # #
        # pre.next = None
        # right_node.next = None

        # reverse_listnode(reverse_node)
        # pre.next = right_node
        # reverse_node.next = curr

        # return dummy_node.next
    
        ## 单次遍历 头插法
        dummy_node = ListNode(-1)
        dummy_node.next = head

        pre = dummy_node

        for i in range(left - 1):
            pre = pre.next

        # print(pre)

        curr = pre.next
        for _ in range(right - left):

            next = curr.next
            curr.next = next.next
            next.next = pre.next
            pre.next = next

        return dummy_node.next




