# date: 2022-07-05
# author: yuchuchu, yangyoupeng

"""
用户管理中心-广义新用户-日规划
"""
import time
import datetime
import calendar
from datetime import timedelta
import math
import sys
import os
import numpy as np

# old_time放在程序运行开始的地方
old_time = time.time()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, MapType
from pyspark.sql.functions import col, lit, array
from pyspark.sql.functions import udf

# # import os
# # os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"


# spark = (SparkSession \
#     .builder \
#     .appName("test-dockerlinuxcontainer") \
#     .enableHiveSupport() \
#     .config("spark.sql.shuffle.partitions", "1000") \
#     .getOrCreate())

# spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
# spark.conf.set("hive.exec.dynamic.partition", "true")
# spark.conf.set("hive.exec.dynamic.partition.mode", "true")
# spark.sql("""ADD JAR hdfs://ns1009/user/mart_jypt/mart_jypt_usr_grow/liuyang266/hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar""")
# spark.sql("""CREATE TEMPORARY FUNCTION hash_sub AS 'com.jd.bdptools.ly.HashMonthly'""")

# # applicationId:spark程序的唯一标识符，其格式取决于调度程序的实现
# app_id = spark.sparkContext.applicationId 
# print(app_id)
# print(spark.version)
cum_probability = np.array([[0.10335037,0.03985507,0.17425432,0.,0.07294118,0.138833]
                            ,[0.18270869,0.07246377,0.27629513,0.,0.13176471,0.17303823]
                            ,[0.2423339,0.09299517,0.34850863,0.66666667,0.17529412,0.19315895]
                            ,[0.29386712,0.12801932,0.42543171,0.66666667,0.21882353,0.21830986]
                            ,[0.33333333,0.14009662,0.44270016,0.66666667,0.24235294,0.25251509]
                            ,[0.36825667,0.15942029,0.48508634,0.66666667,0.27176471,0.2806841]
                            ,[0.39664963,0.1884058,0.52904239,0.66666667,0.29882353,0.31187123]
                            ,[0.42887564,0.20652174,0.57299843,0.66666667,0.32352941,0.34104628]
                            ,[0.45528109,0.24275362,0.58712716,0.66666667,0.35882353,0.36217304]
                            ,[0.48495173,0.26570048,0.59654631,0.66666667,0.39529412,0.37927565]
                            ,[0.50695627,0.29227053,0.60753532,0.66666667,0.43058824,0.40744467]
                            ,[0.5330778,0.32850242,0.62166405,0.66666667,0.45294118,0.42454728]
                            ,[0.5552243,0.34057971,0.64207221,0.66666667,0.49058824,0.44567404]
                            ,[0.57637706,0.36594203,0.67503925,0.66666667,0.50941176,0.4668008,]
                            ,[0.59923339,0.39492754,0.69387755,0.66666667,0.54823529,0.51509054]
                            ,[0.63202726,0.43719807,0.71742543,0.66666667,0.57529412,0.54225352]
                            ,[0.6568711,0.45289855,0.74568289,0.66666667,0.59882353,0.56237425]
                            ,[0.67915957,0.48309179,0.76609105,0.66666667,0.63176471,0.57344064]
                            ,[0.7044293,0.51328502,0.77708006,0.66666667,0.67647059,0.59356137]
                            ,[0.7245883,0.53864734,0.79905808,0.66666667,0.70117647,0.61267606]
                            ,[0.75014196,0.55555556,0.82103611,0.66666667,0.71764706,0.63782696]
                            ,[0.7673197,0.58333333,0.82574568,0.66666667,0.73764706,0.65291751]
                            ,[0.79358319,0.62801932,0.83830455,0.66666667,0.76,0.67102616]
                            ,[0.82027257,0.67270531,0.84929356,0.66666667,0.78352941,0.69919517]
                            ,[0.84085747,0.7089372,0.8744113,0.66666667,0.81294118,0.73038229]
                            ,[0.86697899,0.74516908,0.88540031,0.66666667,0.82823529,0.75653924]
                            ,[0.89040318,0.79589372,0.91522763,0.66666667,0.86588235,0.79476861]
                            ,[0.91993186,0.85024155,0.95290424,0.66666667,0.89411765,0.85412475]
                            ,[0.93796139,0.90700483,0.96546311,0.66666667,0.91529412,0.8943662]
                            ,[0.96919364,0.96497585,0.97645212,0.66666667,0.95294118,0.95171026]
                            ,[1.,1.,1.,1.,1.,1.]])

def get_monthplan_data():
    data = 1
    return data

def get_channel_cum():
    cum_probability = 1
    return cum_probability

def tb_day_plan(natural_visit_cnt_month, com_visit_cnt_month, free_visit_cnt_month, jd_strong_ctrl_visit_cnt_month, jd_normal_ctrl_visit_cnt_month):
    
    
    channel_type = ['natural','com','free','jd_strong_ctrl', 'jd_normal_ctrl']
    channel_month_cnt_dict = {'natural':natural_visit_cnt_month, 
                              'com':com_visit_cnt_month, 
                              'free':free_visit_cnt_month, 
                              'jd_strong_ctrl':jd_strong_ctrl_visit_cnt_month, 
                              'jd_normal_ctrl':jd_normal_ctrl_visit_cnt_month
                              }

    print("channel_month_cnt_dict",channel_month_cnt_dict)
    # 初始化
    # TODO 修改为本月剩余天数
    day_cnt_dict = {'natural':np.zeros(31), 
                    'com':np.zeros(31), 
                    'free':np.zeros(31), 
                    'jd_strong_ctrl':np.zeros(31), 
                    'jd_normal_ctrl':np.zeros(31)
                    }
    print("day_cnt_dict",day_cnt_dict)
    
    for channel in channel_month_cnt_dict:
        # 渠道月规划
        channel_monthcnt = channel_month_cnt_dict[channel]
        # 渠道同比概率
        channel_cum_probability = cum_probability[:,channel_type.index(channel)]
        print("channel_cum_probability:",channel_cum_probability)
        init_channel_daycnt = day_cnt_dict[channel]
        np.random.seed(channel_type.index(channel))
        
        if channel_monthcnt >= 30:
            # TODO 修改为本月剩余天数
            channel_dayplan_list = np.array([1 for i in range(31)]) # 每天置1
        else:
            channel_dayplan_list = monthcnt_to_daycnt(init_channel_daycnt, 
                                                      channel_monthcnt, 
                                                      channel_cum_probability) 
            
        day_cnt_dict[channel] = list(map(float,channel_dayplan_list))
    
    return day_cnt_dict
    
def monthcnt_to_daycnt(init_channel_daycnt, channel_monthcnt, channel_cum_probability):
    """_summary_

    Args:
        init_channel_daycnt (_type_): _description_
        channel_monthcnt (_type_): _description_
        channel_cum_probability (_type_): _description_

    Returns:
        _type_: _description_
    """
    channel_daycnt = init_channel_daycnt
    
    if channel_monthcnt > 0 :
        day_select = np.zeros((1, channel_monthcnt))
        # 生成natural_cnt个随机数 用来判断拆到哪些天
        rand_seed = np.random.rand(1, channel_monthcnt)
        for i in range(channel_monthcnt):
            # 将随机数和累积分布挨个对比，概率大的那天区间也大，被选中的概率自然大
            for j in channel_cum_probability:
                if j >=  rand_seed[0,i]: 
                    day_select[0,i] = channel_cum_probability.tolist().index(j)+1
                    break
        # 微调，作用是若有的天被分配大于1次，则将这天重新打散分配，用create_uniques函数
        print("day_select:", day_select)
        day_select = create_uniques(np.array(day_select[0]).astype('int64'))
        day_idx = np.stack(day_select.astype('int64'))
        print("day_idx",day_idx)
        channel_daycnt[day_idx-1] = 1
    else:
        pass
        
    return channel_daycnt


def create_uniques(arr):
    """_summary_

    Args:
        arr (_type_): _description_

    Returns:
        _type_: _description_
    """
    unq,c = np.unique(arr,return_counts=1)

    m = np.isin(arr,unq[c>1])

    newvals = np.setdiff1d(np.arange(31),arr[~m])
    np.random.shuffle(newvals)
    
    cnt = m.tolist().count(True)
    newvals = newvals[:cnt]
    arr[m] = newvals
    
    return arr

def data_insert_table(data ,table_name ,table_dt ,tabel_dp):

    #result_data = data[table_cols]
    result_data = data
    result_data.createOrReplaceTempView('result_data_tmp')

    
    sql = """
       INSERT OVERWRITE TABLE {table_name} PARTITION (dt='{part_dt}', priority_type, dp)
       -- CREATE table {table_name} as

        select
         *,
         '{part_dp}' as dp
        from
        result_data_tmp
    """.format(table_name=table_name, part_dt=table_dt, part_dp=tabel_dp)
    print(sql)
    spark.sql(sql)
    print("successful data insert tabel")
    
    return 

if __name__ == "__main__":
    # 随机种子

    cum_probability = np.zeros((31, 6))