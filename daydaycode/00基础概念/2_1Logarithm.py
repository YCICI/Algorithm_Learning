import random
import numpy as np


class LogArithm():

    def init_arr(self, size, value):
        """
        return size长度的array, 取值区间为[-value, value]
        """

        return np.random.randint(0, value, size = size) - np.random.randint(0, value, size = size)
    

    def base_func(self, arr):
        """
        绝对正确的方法，暴力求解
        """
        return sorted(arr)
    

    def customize_func(self, arr):
        """
        自定义方法
        """
        


        return arr
    
    def compare(self, size, value, times):
        succeed = "True"
        for _ in range(times):
            init_arr = self.init_arr(size, value)
            base_arr = self.base_func(init_arr)
            customize_arr= self.customize_func(init_arr)
            if base_arr != customize_arr:
                succeed = f"False\r\nbase_arr:{base_arr}\r\ncustomize_arr:{customize_arr}"
                break
        print(succeed)


if __name__ == '__main__':

    LogArithm.compare(5, 10, 10)