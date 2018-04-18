import talib
import numpy as np
import pandas_datareader.data as web
from datetime import datetime
import json
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import codecs
import time
import os
import mysql.connector

# read JSON data
print("start reading json file")
with open("fund2317_secData.json") as input_file:
    Raw_data = json.load(input_file)
print("done reading json file!")

## get data ##
def GetOneData(name, attribute='Close', start_date=datetime(2017,1,1), end_date=datetime(2017,12,31)):
    return np.array([float(d[attribute]) for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)])

def GetData(name, attribute1='Open', attribute2='High', attribute3='Close', attribute4='Low', start_date=datetime(2017,1,1), end_date=datetime(2017,12,31)):
    return np.array([float(d[attribute1]) for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)]),\
           np.array([float(d[attribute2]) for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)]),\
           np.array([float(d[attribute3]) for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)]),\
           np.array([float(d[attribute4]) for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)])

def GetTime(name, attribute='Time', start_date=datetime(2017,1,1), end_date=datetime(2017,12,31)):
    return np.array([d[attribute] for d in Raw_data if (d['Name'] == name) and (parse(d['Time']) >= start_date)and (parse(d['Time']) <= end_date)])

def GetLastDate(name):
    return np.array([d['Time'] for d in Raw_data if (d['Name'] == name)].pop())

def GetFirstDate(name):
    return np.array([d['Time'] for d in Raw_data if (d['Name'] == name)].pop(0))
## end get data ##

## get data from web ##
# def get_close_data(sid):
#     df = web.DataReader(sid,"google",datetime(2017,1,1))
#     close = np.asarray(df['Close'])        # convert series to 2d-array
#     return close
#
# def get_high_data(sid):
#     df = web.DataReader(sid, "google", datetime(2017, 1, 1))
#     high = np.asarray(df['High'])  # convert series to 2d-array
#     return high
#
# def get_low_data(sid):
#     df = web.DataReader(sid, "google", datetime(2017, 1, 1))
#     low = np.asarray(df['Low'])  # convert series to 2d-array
#     return low
## end get data from web ##

## TA index ##
##### MA BIAS RSI MACD KD WR MOM PSY ######
def MA(close, timeperiod, matype=0):
    # matype: 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA, 7=MAMA, 8=T3 (Default=SMA)
    return talib.MA(close, timeperiod, matype)

def BIAS(close, timeperiod=12):
    malist = MA(close, timeperiod)
    biaslist = []
    biaslist = np.append(biaslist, [np.nan] * (timeperiod - 1))
    datalist = range(len(close))

    for item in datalist:
        if item - timeperiod >= -1:
            tmp = (close[item] - malist[item]) / malist[item] * 100
            biaslist = np.append(biaslist, tmp)
    return biaslist

def RSI(close, timeperiod=14):
    return talib.RSI(close, timeperiod)

def MACD(close, fastperiod=12, slowperiod=26, signalperiod=9):
    macd, macdsignal, macdhist = talib.MACD(close, fastperiod, slowperiod, signalperiod)
    # macd = 12天EMA - 26天EMA
    # macdsignal = 9天MACD的EMA
    # macdhist = MACD - MACDsignal
    return macd

def KD(high, low, close, fastk_period=9, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0):
    K, D = talib.STOCH(high, low, close, fastk_period, slowk_period, slowk_matype, slowd_period, slowd_matype)
    return K, D

def WR(high, low, close, timeperiod=14):
    wr = talib.WILLR(high, low, close, timeperiod)
    return wr

def MOM(close, timeperiod=10):
    mom = talib.MOM(close, timeperiod)
    return mom

def PSY(close, timeperiod=12):
    psylist = []
    psylist = np.append(psylist, [np.nan] * timeperiod)
    datalist = range(len(close))

    for item in datalist:               # from 0 to 250 --> total:251 data
        if item - timeperiod >= 0:      # 前timeperiod筆為na
            UPn = 0                     # UPn 值為n日內行情上漲的天數:當日close > 前一日close
            for i in range(item - 1, item - timeperiod, -1):
                if close[i] > close[i-1]:
                    UPn+=1
            tmp = float(UPn) / float(timeperiod) * 100
            psylist = np.append(psylist, tmp)
    return psylist

## end TA index ##

def profit_rate(close, timeperiod=90):      # calculate收益率： (未來90筆 - 今日close) / 今日close
    close_count = len(close)
    tmp = np.roll(close, -timeperiod)
    prolist = (tmp - close) / close
    prolist[(close_count - timeperiod):] = [np.nan] * timeperiod
    return prolist

def profit_rate_rank(profit_rate):      # 收益率分級：by profit_rate
    rank = -1
    if profit_rate >= 0.2:
        rank = 21
    elif 0.20 > profit_rate >= 0.18:
        rank = 20
    elif 0.18 > profit_rate >= 0.16:
        rank = 19
    elif 0.16 > profit_rate >= 0.14:
        rank = 18
    elif 0.14 > profit_rate >= 0.12:
        rank = 17
    elif 0.12 > profit_rate >= 0.10:
        rank = 16
    elif 0.10 > profit_rate >= 0.08:
        rank = 15
    elif 0.08 > profit_rate >= 0.06:
        rank = 14
    elif 0.06 > profit_rate >= 0.04:
        rank = 13
    elif 0.04 > profit_rate >= 0.02:
        rank = 12
    elif 0.02 > profit_rate >= 0.00:
        rank = 11
    elif 0.00 > profit_rate >= -0.02:
        rank = 10
    elif -0.02 > profit_rate >= -0.04:
        rank = 9
    elif -0.04 > profit_rate >= -0.06:
        rank = 8
    elif -0.06 > profit_rate >= -0.08:
        rank = 7
    elif -0.08 > profit_rate >= -0.10:
        rank = 6
    elif -0.10 > profit_rate >= -0.12:
        rank = 5
    elif -0.12 > profit_rate >= -0.14:
        rank = 4
    elif -0.14 > profit_rate >= -0.16:
        rank = 3
    elif -0.16 > profit_rate >= -0.18:
        rank = 2
    elif -0.18 > profit_rate >= -0.20:
        rank = 1
    elif -0.20 > profit_rate:
        rank = 0
    return int(rank)

def set_pr_rank(profit_rate_arr):
    ranklist = []
    profit_rate_arr_count = len(profit_rate_arr)
    nan_count = len(profit_rate_arr[np.isnan(profit_rate_arr)])
    for i in range(0, profit_rate_arr_count - nan_count):
        rank_tmp = profit_rate_rank(profit_rate_arr[i])
        ranklist = np.append(ranklist, rank_tmp)
    ranklist = np.append(ranklist, [np.nan] * nan_count)

    return ranklist

## test
close_2317 = GetOneData('2317','Close', datetime(2017,1,12,9,0),datetime(2017,9,30,13,30))
profit_2317 = profit_rate(close_2317)
profit_rate_rank_2317 = map(profit_rate_rank, profit_2317)

#end test#

### 製作矩陣
# 欄位：  open  high  close  low  MA  EMA  BIAS  RSI  MACD  MOM  PSY
open_2317, high_2317, close_2317, low_2317 = GetData('2317','Open','High','Close','Low', datetime(2017,1,12,9,0),datetime(2017,9,30,13,30))
ma10_2317 = MA(close_2317, 10 , 0)    # set MA10
ema10_2317 = MA(close_2317, 10 , 1)
bias_2317 = BIAS(close_2317)
rsi_2317 = RSI(close_2317)
macd_2317 = MACD(close_2317)
mom_2317 = MOM(close_2317)
psy_2317 = PSY(close_2317)

# vstack to an arr
arr = np.vstack((open_2317, high_2317, close_2317, low_2317, ma10_2317, ema10_2317, bias_2317, rsi_2317, macd_2317, mom_2317, psy_2317))
final_arr = np.transpose(arr)
print(type(final_arr))

## save to json ##
final_arr = final_arr.tolist()
json.dump(final_arr, codecs.open('/Users/ivy/desktop/data.json', 'w', encoding='utf-8'), separators=(',', ':'), indent=4) # saves the array in .json formal
## end save to json ##
