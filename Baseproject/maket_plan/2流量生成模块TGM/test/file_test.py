# date: 2022-04-20
# updta: 2022-06-10
# author: niujianxing,yuchuchu

"""
用户管理中心-算法输出中间表
"""
import time
import datetime
import calendar
from datetime import timedelta
import math
import sys
import os
#import config 

# old_time放在程序运行开始的地方
old_time = time.time()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, MapType
from pyspark.sql.functions import col, lit, array
from pyspark.sql.functions import udf


# import os
# os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"


spark = (SparkSession \
    .builder \
    .appName("test-dockerlinuxcontainer") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "1000") \
    .getOrCreate())

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "true")
spark.sql("""ADD JAR hdfs://ns1009/user/mart_jypt/mart_jypt_usr_grow/liuyang266/hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar""")
spark.sql("""CREATE TEMPORARY FUNCTION hash_sub AS 'com.jd.bdptools.ly.HashMonthly'""")

# applicationId:spark程序的唯一标识符，其格式取决于调度程序的实现
app_id = spark.sparkContext.applicationId 
print(app_id)
print(spark.version)

# 获取sql文件
def  get_sql(sql_file,part_dt,table_name):
    with open (sql_file,'r') as f:
        sql=f.read()
        sql=sql.format(part_dt = part_dt,
                       table_name = table_name,)
        sql = sql.replace('‘', '\`')
    return sql

def get_data(sql_file,part_dt,table_name):
    used_sql = get_sql(sql_file,part_dt,table_name)
    print(used_sql)
    data = spark.sql(used_sql)
    print(data.columns)
    
    return data

if __name__ == '__main__':
    sql_file = 'get_base_data.sql'

    data = get_data(sql_file = sql_file,
                       part_dt= '2022-06-17',
                       table_name = None)
    print(data.columns)
    
    data2 = get_data(sql_file = 'get_lastday_data.sql',
                       part_dt= '2022-06-17',
                       table_name = None)
    print(data2.columns)
