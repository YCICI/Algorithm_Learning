## 总结

### 一、代码

```python
class ListNode():
    def __init__(self,val):
        self.val = val
        self.next = None

root = ListNode(0)
root.next = ListNode(1)
root.next.next = ListNode(2)
print(root.val,root.next.val,root.next.next.val)

```

### 二、时间空间复杂度

时间复杂度 
    
> 插入：  O(1)    
> 删除：  O(1)    
> 查询：  O(n)    

空间复杂度

> O(n)  


### 三、常见题目

基础coding  
>   打印链表公共部分    
>   206反转链表 
>   

算法coding

> 中等24两两交换链表中的节点.py   
> 困难25K个一组翻转链表.py    
> 简单141环形链表.py  
> 简单142环形链表II.py  
> 简单160相交链表.py    
