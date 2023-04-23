import heapq

def smallet_queue(arr):
    """
    默认为最小优先队列
    """
    print("原始数组：{0}".format(arr))
    # 将给定的列表转化为最小堆，线性时间O(n) 
    heapq.heapify(arr)
    print("转换为小顶堆后：",arr)
    
    # 插入元素
    heapq.heappush(arr,2)
    print("插入新元素后：%s"%(arr))
    
    # 弹出最小元素
    min_num = heapq.heappop(arr)
    print("弹出的最小元素：%s"%(min_num))
    print("弹出最小元素后：%s"%(arr))
    
    # 返回最小元素
    print("输出最小元素：%s"%(arr[0]))
    
    # 弹出最小元素，并插入一个新的元素，相当于先heappop，在heappush
    min_num2 = heapq.heapreplace(arr,0)
    print("第二次弹出的最小元素：%s"%(min_num2))
    print("现在的堆结构为: %s"%(arr))

    # 插入新的元素，然后弹出最小元素，相当于先heappush,再headpop
    num = 1
    print("插入新的元素： %s"%(num))
    heapq.heappushpop(arr, num)
    print("现在的堆结构为: %s"%(arr))

def largest_queue(arr):
    """
    最大优先队列
    参照源码https://github.com/python/cpython/blob/3.11/Lib/heapq.py
    """

    # 堆化
    heapq._heapify_max(arr)
    print("构建大根堆：", arr)

    # 弹出
    max_num = heapq._heappop_max(arr)
    print("弹出大根堆堆顶：", max_num)
    print("当前堆结构：", arr)
    

    # 弹出并插入
    insert_num = 3
    heapq._heapreplace_max(arr, insert_num)
    print("插入元素：", insert_num)
    print("当前堆结构：", arr)


if __name__ == '__main__':

    arr = [4, 1, 3, 5, 8]
    smallet_queue(arr)
    print("*" * 20)
    largest_queue(arr)

