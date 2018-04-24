#-*-coding:utf-8 -*-

from calculateAllTa import load_arr
from calculateAllTa import all_ta

# file: time_10.npy、time_10_name.npy

print("start......")

origin_arr = load_arr.load_npy_array('time_10.npy')
# print(origin_arr)
slice_arr = load_arr.slice_arr(origin_arr, 2)
# print(slice_arr)
ma10_arr = all_ta.all_MA(slice_arr, 10)
print(ma10_arr)
rsi14_arr = all_ta.all_RSI(slice_arr)
print(rsi14_arr)

