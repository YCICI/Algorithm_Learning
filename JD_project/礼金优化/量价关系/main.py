import numpy as np
import matplotlib.pyplot as plt

# 设置x的范围，例如从0.1到10
x = np.linspace(2, 10, 100)  # 从0.1开始，避免log(0)的未定义情况

# 定义常数a, b, c
a = 10000000
b = 10
c = 2

# 计算y值
y = a / np.log(b) * np.log(x) + c

# 绘制函数
plt.figure(figsize=(10, 5))
plt.plot(x, y, label=f'y = {a} / log({b}) * log(x) + {c}', color='blue', linestyle='-', linewidth=2)

# 添加图表元素
plt.xlabel('x')
plt.ylabel('y')
plt.title('Log-Log Function with Opposite Trend')
plt.legend()
plt.grid(True)

# 显示图表
plt.show()