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


class LargeHeapq():
    """
    自定义堆
    """
    def heapqinsert(index, nums):
        while nums[index] > nums[int((index - 1) / 2)]:
            nums[index], nums[int((index - 1) / 2)] = nums[int((index - 1) / 2)], nums[index]
            index = (int((index - 1) / 2))
        return 
    
    def heapify(index, nums, size):
        # 备注：自定义函数的使用和
        # print("index=%s, nums=%s, size=%s"%(index, nums, size))

        # 
        left_index = index * 2 + 1
        while left_index <= size:
            # print("left_index: ", left_index)
            # 左右孩子比较
            right_index = left_index + 1
            largest_index = left_index
            if right_index <= size and nums[right_index] > nums[left_index]:
                largest_index = right_index 
            
            # print("largest_index: ", largest_index)
            # 大孩子和父孩子比较
            largest_index = index if nums[index] > nums[largest_index] else largest_index
            if largest_index == index:
                break
            
            nums[largest_index], nums[index] = nums[index], nums[largest_index]

            # 迭代
            index = largest_index
            left_index = index * 2 + 1
            

        return nums



if __name__ == '__main__':

    arr1 = [3,2,3,1,2,4,5,5,6]
    # smallet_queue(arr)
    ######################################################
    print("*" * 20, "base 函数", "*" * 20)
    print("大根堆")
    nums = arr1
    heapq._heapify_max(nums)
    print(nums)

    print( "排序")
    nums = arr1[:]
    heapq.heapify(nums)
    sorted_nums = []

    while nums:
        sorted_nums.append(heapq.heappop(nums))
    print(sorted_nums)

    ########################################################
    print("*" * 20, "only heapify", "*" * 20)
    print("大根堆")
    nums = arr1
    for i in range(len(nums) - 1 , -1, -1):
        LargeHeapq.heapify(i, nums, len(nums) - 1)
    print(nums)
    
    # 堆可以理解为完全二叉树 即每一个父节点都有完整的叶子节点 且父节点为最大or 最小的值
    # 自定义的 heapify 是处理每一颗子树的大小关系，需要从后往前处理才能完成整个数组的堆化
    # arr[0] 是最大值 后面的排序不一定严格大小，需要弹出后更新size继续排序
    print("堆排")
    nums = arr1
    n = len(nums)
    size = n - 1
    for i in range(size, -1, -1):
            LargeHeapq.heapify(i, nums, size)
    
    ### 写法一
    # while size >= 0:
    #     nums[0], nums[size] = nums[size], nums[0]
    #     size -= 1
    #     LargeHeapq.heapify(0, nums, size)
    ### 写法二
    for j in range(n - 1, 0, -1):
        nums[j], nums[0] = nums[0], nums[j]
        LargeHeapq.heapify(0, nums, j - 1)
    print(nums)

    ########################################################
    print("*" * 20, "only heapinsert", "*" * 20)
    print("大根堆")
    nums = arr1
    for i in range(len(nums)):
        LargeHeapq.heapqinsert(i, nums)
    print(nums)
    print("排序")
    nums = arr1
    #构建k大小的大根堆
    for i in range(n):
        LargeHeapq.heapqinsert(i, nums)
    # 
    for j in range(n - 1, 0, -1):
        nums[0], nums[j] = nums[j], nums[0]
        # print(j, nums)
        # heapinsert(j - 1, nums)
        LargeHeapq.heapify(0, nums, j - 1)
    print(nums)



