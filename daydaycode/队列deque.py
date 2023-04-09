#!/usr/bin/env python
# coding: utf-8

from collections import deque

q = deque()

# 添加元素
q.append('eat')
q.append('sleep')
q.append('code')
print(q)

# leftpop 先进先出R
print("先进先出", q.popleft())


# pop 先进后出
print("先进后出", q.pop())





