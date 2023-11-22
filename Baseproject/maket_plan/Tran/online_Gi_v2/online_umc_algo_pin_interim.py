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


# old_time放在程序运行开始的地方
old_time = time.time()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, MapType
from pyspark.sql.functions import col, lit, array
from pyspark.sql.functions import udf

#自定义
import config

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

def get_base_data(sql_file, part_dt, part_dt_3, part_month, user_type_list, life_type_list):
    """获取sql文件

    Args:
        part_dt (_type_): 表格分区

    Returns:
        _type_: 返回执行sql
    """
    with open (sql_file,'r') as f:
        used_sql=f.read()
        used_sql=used_sql.format(part_dt = part_dt,
                                part_month = part_month,
                                user_type_list = user_type_list,
                                life_type_list = life_type_list,
                                part_dt_3 = part_dt_3)
        used_sql = used_sql.replace('‘', '\`')
    #print(sql)
    print("used sql", used_sql)
    data = spark.sql(used_sql)
    data.cache()
    print("data columns", data.columns)
    print("data dtypes", data.dtypes)

    return data

def get_plan_pra(plan_level):
    """返回不同档位的参数值

    Args:
        plan_level: 方案档位, low, medium, high, max

    Returns:
        _type_: 返回月度clv阈值(month_clv_threshold), 
                返回年度clv阈值year_clv_threshold, 
                返回渠道放大系数channel_max_adjust, 
                返回渠道偏好系数channel_preference
    """
    # 月度clv阈值
    # 年度clv等级
    # 渠道上限调节, 999表示取上限，1.5表示均值的1.5倍，1.2表示均值的1.2倍，等等
    # 渠道倾向 channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']
    
    # plan_pra_dict = {'max' : [50, '1', 999, [1.0, 1.0, 1.0, 1.0,1.0]],
    #                  'high' : [50, '1', 1.5, [1.0, 1.0, 1.0, 1.0,1.0]],
    #                  'medium' : [50, '1', 1.2, [1.0, 1.0, 1.0, 1.0,1.0]],
    #                  'low' : [50, '1', 1.0, [1.0, 1.0, 1.0, 1.0,1.0]]}
    
    if plan_level == 'max':
        month_clv_threshold = 50.0  
        year_clv_threshold = '1'  
        channel_max_adjust = 999  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  
    elif plan_level == 'high':
        month_clv_threshold = 50.0
        year_clv_threshold = '1'  
        channel_max_adjust = 1.5 
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  
    elif plan_level == 'medium':
        month_clv_threshold = 50  
        year_clv_threshold = '1'  
        channel_max_adjust = 1.2  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  
    elif plan_level == 'low':
        month_clv_threshold = 50  
        year_clv_threshold = '1' 
        channel_max_adjust = 1.0  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0] 
    else:
        plan_level = 'low_test'
        month_clv_threshold = 50  
        year_clv_threshold = '1' 
        channel_max_adjust = 1.0  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0] 

    return month_clv_threshold, year_clv_threshold, channel_max_adjust, channel_preference

def add_channel_result_col(data, this_month_end):
    """数据新增列

    Args:
        data(dataframe): 基础数据

    Returns:
        dataframe: 新增分配结果列 的数据
    """
    channel_type = ["com", "free", "strong_ctrl", "normal_ctrl", "no_ctrl"]
    channel_diff_mtd = ["com_diff_mtd", "free_diff_mtd", "strong_ctrl_diff_mtd", "normal_ctrl_diff_mtd", "no_ctrl_diff_mtd"]
    data = data.withColumn("pred_total_dau",lit(0.0))
    
    for index in range(len(channel_type)):
        channel_name = channel_type[index]
        channel_diff_mtd_name = channel_diff_mtd[index]
        # 修正后干预次数（减去当月已完成）
        data = data.withColumn('%s_login_cnt'%channel_name, 
                                         data["result"].getItem(channel_diff_mtd_name))
        # 算法实际分配次数（未减已完成）
        data = data.withColumn('algo_%s_login_cnt'%channel_name, 
                                         data["result"].getItem(channel_name))       
        # DAU
        data = data.withColumn("pred_%s_dau"%channel_name, 
                                         col("%s_login_cnt"%channel_name) * col("%s_login_cnt_per_dau"%channel_name))
        # total_DAU
        data = data.withColumn("pred_total_dau",
                                         col("pred_total_dau") + col("%s_login_cnt_per_dau"%channel_name))
    # 增加自然端的数据
    data = data.withColumn("pred_natural_dau",col("natural_pred_login_cnt_rest") * col("natural_login_cnt_per_dau"))
    data = data.withColumn("pred_total_dau",col("pred_total_dau") + col("pred_natural_dau"))
    

    data = data.withColumn("pred_total_dau", col("pred_total_dau") * col("total_login_dau_ratio"))
    data = data.withColumn('is_solvable', data["result"].getItem("flag"))

    # TODO 优先级 P1
    data = data.withColumn('priority_level', lit(50))
    data = data.withColumn("end_time", lit(this_month_end))  # 本月最后1s
    
    # TODO 保量相关信息传出
    data = data.withColumn('is_guaranteed_by_com', col('com_login_cnt_last_month'))
    data = data.withColumn('is_guaranteed_by_free', col('free_login_cnt_last_month'))
    data = data.withColumn('is_guaranteed_by_strong_ctrl', col('strong_ctrl_login_cnt_last_month'))
    data = data.withColumn('is_guaranteed_by_normal_ctrl', col('normal_ctrl_login_cnt_last_month'))
    data = data.withColumn('is_guaranteed_by_no_ctrl', col('no_ctrl_login_cnt_last_month'))
    
   
    # 算法中间值
    algo_total_info = """{"guaranteed":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0},
                            "channel_max_login_days":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0},
                            "channel_min_login_days":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0},
                            "channel_login_costs":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0},
                            "greedy_result":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0},
                            "result":{"com": 0, "free": 0, "strong_ctrl": 0, "normal_ctrl": 0, "no_ctrl": 0}}"""
    
    data = data.withColumn("algo_total_info", lit(algo_total_info)) 
    
    # 字段名称修正
    # 字段名称修正
    rename_cols_dict = config.cols_dict
    
    print('rename_cols_dict',rename_cols_dict)
    print(data.columns)

    if isinstance(rename_cols_dict, dict):
        for old_name, new_name in rename_cols_dict.items():
            if old_name in list(data.columns):
                print(old_name, new_name)
                data = data.withColumnRenamed(old_name, new_name)
            else:
                print("%s not in data"%old_name)
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
    

    return data

def keep_use_cols(data):
    # 保留部分字段
    keep_cols_list = config.table_cols
    print('keep used cols list = ', keep_cols_list)
    return data[keep_cols_list]

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

def juge_how_allocation(run_day, plan_level, base_data, this_month_end, priority_type):
    """if A1 or A2: 
            if 首日：首日分配 
            elif 7d内: 隔3天: 渠道内调整 
            elif 7天等待期后, 隔3天:渠道间调整  TODO 暂未生效
            else: 不调整
       if B1 B2 or C: 
            if 首日：首日分配 
            elif 7d内: 隔5天: 渠道内调整 
            elif 7天等待期后, 隔5天:渠道间调整  TODO 暂未生效
            else: 不调整

    Args:
        run_day (_type_): _description_
        plan_level (_type_): _description_
        base_data (_type_): _description_
        this_month_end (_type_): _description_
        priority_type (_type_): _description_

    Returns:
        _type_: _description_
    """
    # 运行分区 run_day

    # 分配计划执行时间 = 运行分区 + 1
    plan_day = datetime.datetime.strptime(run_day,'%Y-%m-%d') - timedelta(days=- 1)
    plan_year = plan_day.year
    plan_month = plan_day.month

    # 获取每月首日分配方案时间   
    if plan_month== 5 and run_day < '2022-05-30':
        # 5月是05-15分区 ，05-16开始执行首日方案
        first_allocation_day = datetime.datetime.strptime('2022-05-16','%Y-%m-%d')
        
    else:
        # 其他是每月第一天
        run_month_firstday = datetime.datetime.strftime(datetime.datetime(plan_year, plan_month, 1),'%Y-%m-%d')  
        first_allocation_day = datetime.datetime.strptime(run_month_firstday,'%Y-%m-%d')


    day1_ago_runday = datetime.datetime.strftime(datetime.datetime.strptime(run_day,'%Y-%m-%d') - timedelta(days=1),'%Y-%m-%d')
    delta_day = (plan_day -first_allocation_day).days
    plan_flag = "none"
    val_list = config.udf_func_val_list
    print("val_list", val_list)


    print("本月首日为", first_allocation_day)
    print("分区为",run_day)
    print("计划执行日期为",plan_day)
    print("the delta_day is",delta_day)
    
    if priority_type in ["('gy_new_1')","('gy_new_2')","('gy_new_3')","('gy_new_4')","('gy_new_5')"] :
        if delta_day==0:
            print(">"*10,"进行广义新用户首日分配") 
            plan_flag = "firstday"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
        elif  ((delta_day < 7)& (delta_day%5 == 0)):
            print(">"*10,"进行广义新用户 渠道内调整") 
            plan_flag = "selfchannel_adjust"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
        
        elif ((delta_day >= 7) & ((delta_day-7)%5==0)):
            print(">"*10,"进行广义新用户 渠道间调整") 
            plan_flag = "betweenchannel_adjust"
            base_data = base_data.withColumn('between_channel_adjust',lit(0)) # 渠道间调整待修正 当前不生效
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
        
        else:
            print(">"*10,"非首日, 非调控期，不调整") 
            plan_flag = "no_adjust"
            lastday_data = get_lastday_data_no_allocation(sql_file='get_lastday_data.sql',
                                                          plan_level = plan_level, 
                                                           part_dt = run_day,
                                                           part_dt_1day = day1_ago_runday,
                                                           user_type_list = priority_type)
            final_data = lastday_data
            #final_data = add_channel_result_col(final_data, this_month_end)
            
        
    
    if priority_type == "('A1')":
        if delta_day==0:
            print(">"*10,"进行首日分配") 
            plan_flag = "firstday"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
            
        elif ((delta_day < 7)& (delta_day%3 == 0)):
            print(">"*10,"进行高活用户 渠道内调整") 
            plan_flag = "selfchannel_adjust"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
            
        elif ((delta_day >= 7) & ((delta_day-7)%3==0)):
            print(">"*10,"进行高活用户 渠道间调整") 
            # TODO 渠道间调整
            plan_flag = "betweenchannel_adjust"
            base_data = base_data.withColumn('between_channel_adjust',lit(0)) # 渠道间调整待修正
            final_data = base_data.withColumn('result',get_allocation_result(*val_list ))
            final_data = add_channel_result_col(final_data, this_month_end)
            
        else:
            print(">"*10,"非首日, 非调控期，不调整") 
            plan_flag = "no_adjust"
            lastday_data = get_lastday_data_no_allocation(sql_file='get_lastday_data.sql',
                                                          plan_level = plan_level, 
                                                           part_dt = run_day,
                                                           part_dt_1day = day1_ago_runday,
                                                           user_type_list= priority_type)
            final_data = lastday_data
    
    if priority_type in ["('A2')", "('C')", "('B1')", "('B2')"]:
        if delta_day==0:
            print(">"*10,"进行首日分配") 
            plan_flag = "firstday"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
            
        elif  ((delta_day < 7)& (delta_day%5 == 0)):
            print(">"*10,"进行非高活用户 渠道内调整") 
            plan_flag = "selfchannel_adjust"
            final_data = base_data.withColumn('result',get_allocation_result(*val_list))
            final_data = add_channel_result_col(final_data, this_month_end)
            
        elif ((delta_day >= 7) & ((delta_day-7)%5==0)):
            print(">"*10,"进行非高活用户 渠道间调整") 
            plan_flag = "betweenchannel_adjust"
            base_data = base_data.withColumn('between_channel_adjust',lit(0)) # 渠道间调整待修正 当前不生效
            final_data = base_data.withColumn('result',get_allocation_result(*val_list,
                                                                            'between_channel_adjust'))    
            final_data = add_channel_result_col(final_data, this_month_end)
            
        else:
            print(">"*10,"非首日, 非调控期，不调整") 
            plan_flag = "no_adjust"
            lastday_data = get_lastday_data_no_allocation(sql_file='get_lastday_data.sql',
                                                          plan_level = plan_level, 
                                                           part_dt = run_day,
                                                           part_dt_1day = day1_ago_runday,
                                                           user_type_list= priority_type)
            final_data = lastday_data
            #final_data = add_channel_result_col(final_data, this_month_end)
    # 新增方案类型字段
    final_data = final_data.withColumn('plan_flag',lit(plan_flag))
            
    return final_data, plan_flag

@udf(returnType=MapType(StringType(), IntegerType()))
def get_allocation_result(part_dt,month_clv_threshold,year_clv_threshold,channel_max_adjust,channel_preference,a1_threshold,priority_type,my_hash_code,
                lower_lift_login_cnt, natural_pred_login_cnt_rest,login_cnt_mtd,natural_login_cnt_mtd,no_ctrl_login_cnt_mtd,
                is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl,is_acce_by_no_ctrl,
                com_max_login_count, free_max_login_count, strong_ctrl_max_login_count,normal_ctrl_max_login_count,no_ctrl_max_login_count,
                com_login_cost, free_login_cost, strong_ctrl_login_cost, normal_login_cost,no_ctrl_login_cost, 
                com_quality_coeff,free_quality_coeff,strong_ctrl_quality_coeff,normal_ctrl_quality_coeff,no_ctrl_quality_coeff,
                com_login_cnt_per_dau, free_login_cnt_per_dau,  strong_ctrl_login_cnt_per_dau, normal_ctrl_login_cnt_per_dau, no_ctrl_login_cnt_per_dau,
                clv_pred_1m,gmv_year,
                login_cnt_last_month, natural_login_cnt_last_month,com_login_cnt_last_month,free_login_cnt_last_month,strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_month,
                com_login_cnt_mtd,free_login_cnt_mtd,strong_ctrl_login_cnt_mtd,normal_ctrl_login_cnt_mtd,month_hash,
                original_class, original_class_tg, original_class_sk, original_class_jzt,user_class,
                com_cnt_rate=1, free_cnt_rate=1, strong_cnt_rate=1, normal_cnt_rate=1,nocontrol_cnt_rate=1, 
                between_channel_adjust = 0):
        # 6月方案临时处理
    this_month_end_day = get_this_month_end_day(part_dt)
    #上月取mtd
    if part_dt == this_month_end_day:
        login_cnt_last_month = login_cnt_mtd
        natural_login_cnt_last_month = natural_login_cnt_mtd
        com_login_cnt_last_month = com_login_cnt_mtd
        free_login_cnt_last_month = free_login_cnt_mtd
        strong_ctrl_login_cnt_last_month = strong_ctrl_login_cnt_mtd
        normal_ctrl_login_cnt_last_month = normal_ctrl_login_cnt_mtd
        no_ctrl_login_cnt_last_month = no_ctrl_login_cnt_mtd
        
        #mtd置零
        login_cnt_mtd = 0 
        natural_login_cnt_mtd = 0
        com_login_cnt_mtd = 0
        free_login_cnt_mtd = 0
        strong_ctrl_login_cnt_mtd = 0
        normal_ctrl_login_cnt_mtd = 0
        no_ctrl_login_cnt_mtd = 0
    
    # 上月干预次数 乘以大促系数
    if part_dt <= '2022-06-26' and part_dt>='2022-05-31':
        # 大促月才有放大系数
        com_login_cnt_last_month = sampling_supplement(channel_days = com_login_cnt_last_month,
                                                        adjust_day = com_login_cnt_last_month * com_cnt_rate,
                                                        user_class= original_class )
        
        free_login_cnt_last_month = sampling_supplement(channel_days = free_login_cnt_last_month,
                                                        adjust_day = free_login_cnt_last_month * free_cnt_rate,
                                                        user_class= original_class )
        
        strong_ctrl_login_cnt_last_month = sampling_supplement(channel_days = strong_ctrl_login_cnt_last_month,
                                                        adjust_day = strong_ctrl_login_cnt_last_month * strong_cnt_rate,
                                                        user_class= original_class )
        normal_ctrl_login_cnt_last_month = sampling_supplement(channel_days = normal_ctrl_login_cnt_last_month,
                                                        adjust_day = normal_ctrl_login_cnt_last_month * normal_cnt_rate,
                                                        user_class= original_class )
        no_ctrl_login_cnt_last_month = sampling_supplement(channel_days =no_ctrl_login_cnt_last_month,
                                                        adjust_day = no_ctrl_login_cnt_last_month * nocontrol_cnt_rate,
                                                        user_class= original_class )

    # 用户价值
    clv = get_user_clv(clv_pred_1m)
    user_label = priority_type
    
    # 第一层 基础保量
    basic_result = BasicGuarantee(com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month,
                                  normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month)
    
    # 第二层 业务规则  输出业务结果以及对阶跃渠道上限处理
   
    businessrule_result, businessrule_no_allocation_channel, channel_businessrule_max, channel_businessrule_max_add = BusinessRuleSupplement(basic_result, user_label,  year_clv_threshold, gmv_year, clv, month_clv_threshold, 
                                                com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month,
                                                normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month, 
                                                my_hash_code, month_hash, 
                                                original_class, original_class_tg, original_class_sk, original_class_jzt,user_class)
    
    # 第三层 贪心求解阶跃
    # 目标 TODO 获取目标 渠道上限 以及成本写为一个类
    # part_dt,lift_login, natura_login, current_login,no_ctrl_login
    channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']
    target_login_days = get_target_allocation_login(part_dt=part_dt,
                                                    lift_login=lower_lift_login_cnt,
                                                    natura_login=natural_pred_login_cnt_rest , #用上月自然登录次数替代本月预测v4 (natural_login_cnt_last_month - natural_login_cnt_mtd)
                                                    current_login=login_cnt_mtd,
                                                    no_ctrl_login=(no_ctrl_login_cnt_last_month - no_ctrl_login_cnt_mtd))

    # 获取阶跃渠道上限 & 参数调整（业务加量以及渠道放大）
    
    channel_max_login_days   = get_user_channel_max_func(part_dt, priority_type, my_hash_code, channel_businessrule_max_add, channel_businessrule_max,  
                                                            com_max_login_count, free_max_login_count, strong_ctrl_max_login_count, normal_ctrl_max_login_count,no_ctrl_max_login_count,
                                                            com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month, normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                                                            is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl, is_acce_by_no_ctrl,
                                                            month_clv_threshold,channel_max_adjust,channel_preference, 
                                                            com_login_cnt_mtd,free_login_cnt_mtd,strong_ctrl_login_cnt_mtd,normal_ctrl_login_cnt_mtd,no_ctrl_login_cnt_mtd,between_channel_adjust,
                                                            original_class,user_class)
    # 渠道成本
    channel_login_costs = get_channel_login_costs(channel_type, com_login_cost, free_login_cost, strong_ctrl_login_cost, normal_login_cost, no_ctrl_login_cost,
                                                  com_login_cnt_per_dau, free_login_cnt_per_dau, strong_ctrl_login_cnt_per_dau, normal_ctrl_login_cnt_per_dau, no_ctrl_login_cnt_per_dau,
                                                  com_quality_coeff, free_quality_coeff, strong_ctrl_quality_coeff, normal_ctrl_quality_coeff, no_ctrl_quality_coeff)

    # 求解
    greedy_result = GreedySolver(target_login_days, businessrule_no_allocation_channel, businessrule_result, 
                                 channel_max_login_days,  channel_login_costs)

    # 根据用户类型修正渠道分配方案    
    # 结果最终修正1 不小于上月用户次数  
    final_result = greedy_result
    final_result['com'] = math.ceil(max(final_result['com'],com_login_cnt_last_month))
    final_result['free'] = math.ceil(max(final_result['free'],free_login_cnt_last_month))
    final_result['strong_ctrl'] = math.ceil(max(final_result['strong_ctrl'],strong_ctrl_login_cnt_last_month))
    final_result['no_ctrl'] = math.ceil(max(final_result['no_ctrl'],no_ctrl_login_cnt_last_month))
   
    # 结果最终修正3 减去渠道已完成
    final_result['com_diff_mtd'] = math.ceil(max(final_result['com'] - com_login_cnt_mtd, 0))
    final_result['free_diff_mtd'] = math.ceil(max(final_result['free'] - free_login_cnt_mtd, 0))
    final_result['strong_ctrl_diff_mtd'] = math.ceil(max(final_result['strong_ctrl'] - strong_ctrl_login_cnt_mtd, 0))
    final_result['no_ctrl_diff_mtd'] = math.ceil(max(final_result['no_ctrl'] - no_ctrl_login_cnt_mtd, 0))    
        
    ## 修正2 可控置零
    if user_label == 'A1':
        final_result['normal_ctrl'] = math.ceil(0)
    else:
        final_result['normal_ctrl'] = math.ceil(max(final_result['normal_ctrl'], normal_ctrl_login_cnt_last_month))
        # 结果最终修正3 减去渠道已完成
        final_result['normal_ctrl_diff_mtd'] = math.ceil(max(final_result['normal_ctrl'] - normal_ctrl_login_cnt_mtd, 0))
 

    re = sum(list(final_result.values()))
    target_flag = 1 if re >= target_login_days else 0
    final_result['flag'] = target_flag       
    return final_result
    
def get_resource_ctl_pra(user_type, user_class):
    """获取各增长指数分级的用户渠道资源倾斜系数

    Args:
        user_type (_type_): 用户类型, A1-高活用户, A2B1B2C-低活用户, gy_new
        user_class (_type_): 用户

    Returns:
        _type_: 各增长指数的资源倾斜系数
    """
    # todo  修改为真实数据
    base = config.resource_pra
    
    print("-"*10,'start get_resource_ctl_pra',"-"*10)
    if user_type in ('gy_new_1','gy_new_2','gy_new_3','gy_new_4','gy_new_5'):
        print("user_type in ('gy_new_1','gy_new_2')")
        print(user_class)
        resource_ctl_pra_dict = base['gy_new'][int(user_class)]
        print(resource_ctl_pra_dict)
    elif user_type in ('A1'):
        print("user_type in ('A1')")
        print(user_class)
        resource_ctl_pra_dict = base['high_active'][int(user_class)]
        print(resource_ctl_pra_dict)
    elif user_type in ('A2','B1','B2','C'):
        print("user_type in ('A2','B1','B2','C')")
        print(user_class)
        resource_ctl_pra_dict = base['low_active'][int(user_class)]
        print(resource_ctl_pra_dict)
    print(sum(resource_ctl_pra_dict.values()))
    print("-"*10,"-"*10)
    
    return resource_ctl_pra_dict

def get_this_month_end_day(dt):
    # 当前运行时间 字符串-> datetime
    now = datetime.datetime.strptime(dt, '%Y-%m-%d')

    # 获取本月最后一天 最后一秒

    # now_month = now.month
    this_month_end =datetime.datetime.strftime(datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]),'%Y-%m-%d') + ' 59:59:59'
    this_month_end_day = datetime.datetime.strftime(datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]),'%Y-%m-%d')

    
    return this_month_end_day

def get_user_clv(clv_pred_1m):
    """_summary_

    Args:
        clv_pred_1m (_type_): _description_

    Returns:
        _type_: _description_
    """
    return max(clv_pred_1m,0)

# 返回值必须全为Int, 否则类型不对的变量会被设为null
# @udf(returnType=MapType(StringType(), IntegerType()))
def BasicGuarantee(com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month, normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month):
    """获取渠道保量(用户上月该渠道引流成功次数,向上取整),渠道保量作为用户渠引流成功下限

    Args:
        com_login_cnt_last_month (_type_): _description_
        free_login_cnt_last_month (_type_): _description_
        strong_ctrl_login_cnt_last_month (_type_): _description_
        normal_ctrl_login_cnt_last_month (_type_): _description_
        no_ctrl_login_cnt_last_month (_type_): _description_

    Returns:
        dict(str:int): 各渠道保量次数
    """
    channel_guaranteed = {'com': 0, 'free': 0, 'strong_ctrl': 0, 'normal_ctrl': 0, 'no_ctrl': 0}
    
    channel_guaranteed['com'] = max(math.ceil(com_login_cnt_last_month),0)
    channel_guaranteed['free'] = max(math.ceil(free_login_cnt_last_month),0)
    channel_guaranteed['strong_ctrl'] = max(math.ceil(strong_ctrl_login_cnt_last_month),0)
    channel_guaranteed['normal_ctrl'] = max(math.ceil(normal_ctrl_login_cnt_last_month),0)
    channel_guaranteed['no_ctrl'] = max(math.ceil(no_ctrl_login_cnt_last_month),0)
    
    return channel_guaranteed

def BusinessRuleSupplement(basic_result, user_label,  year_clv_threshold, gmv_year, clv, month_clv_threshold, 
                           com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month,
                           normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month, 
                           my_hash_code, month_hash,
                           original_class, original_class_tg, original_class_sk, original_class_jzt, user_class):
    """根据用户逻辑进行方案调整

    """
    print("-"*10,'start BusinessRuleSupplement',"-"*10)
    channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']
    businessrule_no_allocation_channel = []
    businessrule_plan = basic_result
    channel_businessrule_max = {} # 业务渠道上限限制
    channel_businessrule_max_add = {'com': 0, 'free': 0, 'strong_ctrl': 0, 'normal_ctrl': 0, 'no_ctrl': 0}
    
   
    year_clv_threshold = 600.0
    resource_ctl_pra_dict = get_resource_ctl_pra(user_type=user_label,
                                                 user_class=user_class)
    resource_ctl_pra_sum = sum(resource_ctl_pra_dict.values())
    print("user_class           :",user_class)
    print("basic_result         :",basic_result)
    print("resource_ctl_pra_dict:",resource_ctl_pra_dict)
    print("resource_ctl_pra_sum :",resource_ctl_pra_sum)

    if user_label == 'A1':
        # 根据业务逻辑调整渠道上限
        com_adjust_pra = 1.2
        supplement_precent = 1 # 100优先级占比15% 需要加量10%的用户
        msg_add = 1
    
        # 京东可控置0 
        businessrule_no_allocation_channel.append('normal_ctrl')

        # 商业化渠道补量
        # A1人群的商业化补量不超过20%
        # 商业化在上月基础上调整1.2倍的抽样分配结果
        com_adjust_day = sampling_supplement(channel_days = com_login_cnt_last_month,
                                             adjust_day = com_adjust_pra * com_login_cnt_last_month, # 1.2
                                             user_class= original_class )
        
        channel_businessrule_max['com'] = com_adjust_day  # A1人群的商业化补量不超过20%
   
        # 免费渠道补量
        businessrule_plan['free'] +=  1 
        channel_businessrule_max_add['free'] +=1
        
        # 短信渠道补量
        if (original_class > (100 - supplement_precent)):  # 取优先级TOP 10%
            businessrule_plan['strong_ctrl'] += msg_add
            channel_businessrule_max_add['strong_ctrl'] += msg_add
            
        else:
            pass
       

    elif user_label in ('A2','B1','B2','C'):
        # 中低活用户
        # 不可控渠道不分配
        businessrule_plan['no_ctrl'] =  no_ctrl_login_cnt_last_month
        print("中低活用户")
        for c_name in resource_ctl_pra_dict:
            if resource_ctl_pra_dict[c_name]==0:
                businessrule_plan[c_name] =  0  # 决策点，给0还是给basic方案
                businessrule_no_allocation_channel += c_name  
                
        
        
     
    elif user_label in ('gy_new_1','gy_new_2','gy_new_3','gy_new_4','gy_new_5'):
        # 广义新用户
        # 不可控渠道不分配
        businessrule_plan['no_ctrl'] =  no_ctrl_login_cnt_last_month
        for c_name in resource_ctl_pra_dict:
            if resource_ctl_pra_dict[c_name]==0:
                businessrule_plan[c_name] =  0  # 决策点，给0还是给basic方案
                businessrule_no_allocation_channel += c_name   
        
        # 广义新兜底策略
        for channel_name in basic_result:
            businessrule_plan[channel_name] = max(1,businessrule_plan[channel_name]) #广义新兜底 各渠道至少为1
            channel_businessrule_max_add[channel_name] += businessrule_plan[channel_name]-basic_result[channel_name]
        
       
    print("businessrule_plan:",businessrule_plan)
    print("businessrule_no_allocation_channel:",businessrule_plan)
    print("channel_businessrule_max:",businessrule_plan)
    print("channel_businessrule_max_add:",businessrule_plan)
    print("-"*20)
                   
    return businessrule_plan, businessrule_no_allocation_channel, channel_businessrule_max, channel_businessrule_max_add

def sampling_supplement(channel_days, adjust_day, user_class):
    """
       举例说明:
       假设该渠道有100人, 渠道干预次数默认值为2, 总干预次数为200
       整体干预次数要提升1.2倍即240, 则每个人的渠道干预次数为2.4, 
       干预次数按照采样的方法新增, 所有人干预次数基础调整为2, 40%的人在2的基础上再增加1次, 保证整体的干预次数是调整了1.2倍
       (如果是hash采样, 则随机hash值大于60的用户进行加量; 
       如果是按照优先级排序, 则优先级分类大于60的用户进行加量)

    Args:
        channel_days (_type_): 渠道干预天数
        adjust_day (double): 需要调整的天数
        user_class (string): 用户hash值/优先级排序等可对用户进行等分100分桶的标签

    Returns:
        _type_: 调整后的渠道干预天数
    """
    print("-"*10,'start sampling_supplement',"-"*10)

    adjust_day = adjust_day * 100 # 2.4
    
    adjust_day_base , adjust_day_precent_add = adjust_day//100 , 100 - int(adjust_day%100)
    
    # print(adjust_day_base , adjust_day_hash_add)
    channel_days = adjust_day_base # 2
    print("user_class", user_class)
    if (user_class >= adjust_day_precent_add) and adjust_day_base>0 : # 0.4 TOP40%用户
        channel_days += 1
    
    print("-"*10,"-"*10)
    
    return channel_days

def get_target_allocation_login(part_dt,lift_login, natura_login, current_login,no_ctrl_login):
    """获取目标登录次数

    Args:
        lift_login (_type_): 阶跃所需引流次数
        natura_login (_type_): 本月自然预测登录次数
        current_login (_type_): 当前登录次数
        no_ctrl_login: 不可控渠道本月登录次数

    Returns:
        _type_: _description_
    """
    # part_dt = '2022-03-31'
    # TODO 当前登录次数要用各渠道*质量归一后求和数据，当前使用的是事实数据
    today =datetime.datetime.strptime(part_dt,'%Y-%m-%d') + timedelta(days=1)
    if today.day==1: 
        # 每月1号方案需要对当前登录次数置零
        current_login = min(current_login,0)
    else:
        pass
    
    return lift_login - current_login - natura_login - no_ctrl_login

def get_user_channel_max_func(part_dt, priority_type,my_hash_code, channel_businessrule_max_add, channel_businessrule_max, 
                    com_max_login_count, free_max_login_count, strong_ctrl_max_login_count, normal_ctrl_max_login_count,no_ctrl_max_login_count,
                    com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month, normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                    is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl, is_acce_by_no_ctrl,
                    month_clv_threshold,channel_max_adjust,channel_preference,
                    com_login_cnt_mtd,free_login_cnt_mtd,strong_ctrl_login_cnt_mtd,normal_ctrl_login_cnt_mtd,no_ctrl_login_cnt_mtd,
                    between_channel_adjust, original_class, user_class):
    """返回用户各渠道上下限
    Args:
        priority_type (str): 用户标签
        com_max_login_count (_type_): 商业化渠道上限(格子)
        free_max_login_count (_type_): 免费渠道上限(格子)
        strong_ctrl_max_login_count (_type_):强控渠道上限(格子)
        normal_ctrl_max_login_count (_type_): 可控渠道上限(格子)
        no_ctrl_max_login_count (_type_):不可控渠道上限(格子)
        com_login_cnt_last_month (_type_):商业化渠道上月干预次数(用户)
        free_login_cnt_last_month (_type_): 免费渠道上月干预次数(用户)
        strong_ctrl_login_cnt_last_month (_type_): 强控渠道上月干预次数(用户)
        normal_ctrl_login_cnt_last_month (_type_):可控渠道上月干预次数(用户)
        no_ctrl_login_cnt_last_month (_type_): 不可控渠道上月干预次数(用户)
        is_acce_by_com (bool): 商业化是否可达
        is_acce_by_free (bool): 免费是否可达
        is_acce_by_strong_ctrl (bool): 强控是否可达
        is_acce_by_normal_ctrl (bool): 可控是否可达
        is_acce_by_no_ctrl (bool): 不可控是否可达
        is_guaranteed_by_com (bool): 商业化保量次数
        is_guaranteed_by_free (bool): _description_
        is_guaranteed_by_strong_ctrl (bool): _description_
        is_guaranteed_by_normal_ctrl (bool): _description_
        is_guaranteed_by_no_ctrl (bool): _description_
        month_clv_threshold (_type_): a人群划分
        channel_max_adjust (_type_): 方案档位调节系数
        channel_preference (_type_): _description_

    Returns:
        channel_max_login_days(dict): 用户各渠道上限
        channel_min_login_days(dict): 用户各渠道下限
    """
    
    # channel_type = ['com','free','strong_ctrl','normal_ctrl','no_ctrl']
    # 获得channel_max_login_days
    is_acce_by_com= is_acce_by_com if is_acce_by_com else 1
    is_acce_by_free= is_acce_by_free if is_acce_by_free else 1
    is_acce_by_strong_ctrl= is_acce_by_strong_ctrl if is_acce_by_strong_ctrl else 1
    is_acce_by_normal_ctrl= is_acce_by_normal_ctrl if is_acce_by_normal_ctrl else 1
    is_acce_by_no_ctrl= is_acce_by_no_ctrl if is_acce_by_no_ctrl else 1

    channel_max_login_days = {'com' :is_acce_by_com,
                              'free':is_acce_by_free,
                              'strong_ctrl':is_acce_by_strong_ctrl,
                              'normal_ctrl':is_acce_by_normal_ctrl,
                              'no_ctrl':is_acce_by_no_ctrl}
    
    channel_login_cnt_last_month = {'com' : com_login_cnt_last_month,
                                    'free': free_login_cnt_last_month,
                                    'strong_ctrl': strong_ctrl_login_cnt_last_month,
                                    'normal_ctrl': normal_ctrl_login_cnt_last_month}
    
    channel_grid_max_login_cnt = {'com' : com_max_login_count, 
                                  'free' : free_max_login_count, 
                                  'strong_ctrl' : strong_ctrl_max_login_count, 
                                  'normal_ctrl' : normal_ctrl_max_login_count,
                                  'no_ctrl' : no_ctrl_max_login_count}
    # 参数2 channel_max_adjust
    
    
    if channel_max_adjust == 999:
        channel_max_login_days['com'] = math.ceil(com_max_login_count) if is_acce_by_com >0 else channel_max_login_days['com'] 
        channel_max_login_days['free'] = math.ceil(free_max_login_count) if is_acce_by_free >0 else channel_max_login_days['free'] 
        channel_max_login_days['strong_ctrl'] = math.ceil(strong_ctrl_max_login_count) if is_acce_by_strong_ctrl >0 else channel_max_login_days['strong_ctrl'] 
        channel_max_login_days['normal_ctrl'] = math.ceil(normal_ctrl_max_login_count) if is_acce_by_normal_ctrl >0 else channel_max_login_days['normal_ctrl'] 
        
    else:
        channel_resource_pra = get_resource_ctl_pra(user_type = priority_type,
                                                    user_class = user_class)
        
        print('start',channel_max_login_days)
        print("根据优先级调整上限")
        
        for c_name in channel_login_cnt_last_month:
            #  上限修正:  1.2*上月* 倾斜系数 >格子上限， 取格子上限； 1.2*上月<=上限，取1.2*上月
            #  抽样法调整渠道上限
            print(">"*5,c_name)
            print("%s优先级系数"%c_name,channel_resource_pra[c_name])
            if (channel_login_cnt_last_month[c_name] * 1.2 * channel_resource_pra[c_name] ) > channel_grid_max_login_cnt[c_name]:
                print("1.2*上月>上限")
                channel_max_login_days[c_name] = channel_grid_max_login_cnt[c_name]
            else:
                print("1.2*上月<=上限")
                channel_max_login_days[c_name] = sampling_supplement(channel_days = channel_max_login_days[c_name],
                                                                     adjust_day = max(1,channel_login_cnt_last_month[c_name]) * 1.2 * channel_resource_pra[c_name],
                                                                     user_class= original_class )
            print('渠道上限',c_name, ':', channel_max_login_days[c_name])
                
    channel_max_login_days['no_ctrl'] = math.ceil(1.0 * no_ctrl_login_cnt_last_month)
    print('2channel_max_login_days',channel_max_login_days)
    # 按业务规则修正渠道上限 不超过业务逻辑上限
    # 业务规则加量则 渠道上限加量
    for channel in channel_max_login_days:
        channel_max_login_days[channel] += channel_businessrule_max_add[channel]
        if channel in channel_businessrule_max:
            channel_max_login_days[channel] = min(channel_max_login_days[channel], channel_businessrule_max[channel])
        else:
            continue
    print('final channel_max_login_days',channel_max_login_days)
    # 增加渠道间调整逻辑
    # 本次上线未生效
    dt = part_dt #dt一般是T+1天 本月完成也是T+1的数据
    now = datetime.datetime.strptime(dt, '%Y-%m-%d')
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1])
    vaild_day_this_month = (this_month_end - now).days
    
    if between_channel_adjust==1:
        channel_login_cnt_mtd = {'com' :com_login_cnt_mtd,
                               'free':free_login_cnt_mtd,
                               'strong_ctrl':strong_ctrl_login_cnt_mtd,
                               'normal_ctrl':normal_ctrl_login_cnt_mtd,
                               'no_ctrl':no_ctrl_login_cnt_mtd}
        
        for adjust_channel in channel_max_login_days:
            if (channel_max_login_days[adjust_channel] - channel_login_cnt_mtd[adjust_channel]<=2) & (channel_login_cnt_mtd[adjust_channel] >= 3):
                channel_max_login_days[adjust_channel] = max(math.ceil(channel_max_login_days[adjust_channel] * 1.1),channel_login_cnt_mtd[adjust_channel]+2)
            
            elif channel_max_login_days[adjust_channel] - channel_login_cnt_mtd[adjust_channel] >= vaild_day_this_month * 1.5 :
                channel_max_login_days[adjust_channel] = max(math.ceil(channel_max_login_days[adjust_channel] * 0.9),math.ceil(vaild_day_this_month * 1.5))
                
    return channel_max_login_days

def get_channel_login_costs(channel_type,
                            com_login_cost,free_login_cost,strong_ctrl_login_cost,normal_login_cost,no_ctrl_login_cost,
                            com_login_cnt_per_dau,free_login_cnt_per_dau,strong_ctrl_login_cnt_per_dau,normal_ctrl_login_cnt_per_dau,no_ctrl_login_cnt_per_dau,
                            com_quality_coeff,free_quality_coeff,strong_ctrl_quality_coeff,normal_quality_coeff,no_ctrl_quality_coeff):
    """  返回渠道成本

    Args:
        channel_type (_type_): _description_
        com_login_cost (_type_): _description_
        free_login_cost (_type_): _description_
        strong_ctrl_login_cost (_type_): _description_
        normal_login_cost (_type_): _description_
        no_ctrl_login_cost (_type_): _description_
        com_login_cnt_per_dau (_type_): _description_
        free_login_cnt_per_dau (_type_): _description_
        strong_ctrl_login_cnt_per_dau (_type_): _description_
        normal_ctrl_login_cnt_per_dau (_type_): _description_
        no_ctrl_login_cnt_per_dau (_type_): _description_
        com_quality_coeff (_type_): _description_
        free_quality_coeff (_type_): _description_
        strong_ctrl_quality_coeff (_type_): _description_
        normal_quality_coeff (_type_): _description_
        no_ctrl_quality_coeff (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    channel_cost = [com_login_cost,free_login_cost,strong_ctrl_login_cost,normal_login_cost,no_ctrl_login_cost]
    channel_pre_dau = [com_login_cnt_per_dau,free_login_cnt_per_dau,strong_ctrl_login_cnt_per_dau,normal_ctrl_login_cnt_per_dau,no_ctrl_login_cnt_per_dau,]
    channel_coeff = [com_quality_coeff,free_quality_coeff,strong_ctrl_quality_coeff,normal_quality_coeff,no_ctrl_quality_coeff]

    dau_cost =[]
    for i in range(len(channel_cost)):
        if (channel_pre_dau[i] > 0 ): 
            # check
            cost = channel_cost[i] / channel_pre_dau[i] / channel_coeff[i]
        else:
            cost = channel_cost[i]
        dau_cost.append(cost)

    return dict(zip(channel_type, dau_cost))


def GreedySolver(target_login_days, businessrule_no_allocation_channel, businessrule_result, 
                channel_max_login_days,  channel_login_costs):

    # channel_type = ['com','free','strong_ctrl','normal_ctrl','no_ctrl']  
    if target_login_days <=0:
        # Todo 以满足阶跃次数的用户可以整体不分配
        print("target_login_days <=0: greedy_plan = businessrule_result")
        greedy_plan = businessrule_result
    
    else:
        print("target_login_days >0: greedy solver")
        # 业务规则置零渠道  
        no_allocation_channel_list = businessrule_no_allocation_channel
        no_allocation_channel_list.append('no_ctrl')
        # 业务规则输出的方案结果
        greedy_plan = businessrule_result

        login_costs_dict = channel_login_costs
        sorted_costs = sorted(login_costs_dict.items(), key=lambda kv: (kv[1], kv[0]))

        for i in range(len(sorted_costs)):    
            
            used_day = 0
            index = sorted_costs[i][0]
            # print("待分配的渠道:",index)
            # 被分配的次数 = 目标次数 - 渠道已分配的次数（保量+业务补量）
            should_target_login_cnt = target_login_days - businessrule_result[index]
            # print("待分配的次数 = target_login_days - businessrule_result[index]:")
            # print("%d = %d - %d"%(should_target_login_cnt,target_login_days, businessrule_result[index]))
            
            # 渠道不可分配，则跳过
            if index in no_allocation_channel_list:
                print("该渠道不可分配")
                continue
            
            # 渠道上限修正为 渠道上限 - 已分配次数
            channel_max_day = math.ceil(channel_max_login_days[index])
            channel_max_day = channel_max_day - businessrule_result[index]
            # print("渠道上限修正为 渠道上限 - 已分配次数")
            # print("%d = %d - %d"%(channel_max_day,math.ceil(channel_max_login_days[index]),businessrule_result[index]))

            used_day = min(channel_max_day, should_target_login_cnt)
            # print("used_day = min(channel_max_day, should_target_login_cnt)")
            # print("%d = min(%d , %d)"%(used_day,channel_max_day,should_target_login_cnt))

            target_login_days -= used_day
            target_login_days = max(0, target_login_days)
            # print("target_login_days:",target_login_days)

            greedy_plan[index] += used_day

    print("greedy_plan:",greedy_plan)
    return greedy_plan

def get_lastday_data_no_allocation(sql_file, plan_level, part_dt, part_dt_1day, user_type_list):
    
    with open (sql_file,'r') as f:
        used_sql=f.read()
        used_sql=used_sql.format(part_dt_1day = part_dt_1day, 
                                 part_dt = part_dt, 
                                 part_dp = plan_level, 
                                 user_type_list = user_type_list)
        used_sql = used_sql.replace('‘', '\`')
    #print(sql)
    print("used sql", used_sql)
        
    print("get lastday data sql", used_sql)
    data = spark.sql(used_sql)
    data.cache()
    print("data columns", data.columns)
    print("data dtypes", data.dtypes)

    return data


if __name__ == '__main__':
    # 获取基础参数
    day_ago_1 =  sys.argv[1] # T-1
    dp = sys.argv[2] #  [low,medium,high,max]
    user_type_list = sys.argv[3] #('A1','A2') 
    life_type_list = sys.argv[4] #('潜在期','流失期') 
    now = datetime.datetime.strptime(day_ago_1, '%Y-%m-%d')
    day_ago_1 =  datetime.datetime.strptime(day_ago_1, '%Y-%m-%d').strftime('%Y-%m-%d')
    day_ago_3 = datetime.datetime.strftime(now - timedelta(days=2),'%Y-%m-%d')
    now_month = now.month
    this_month_end =datetime.datetime.strftime(datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]),'%Y-%m-%d') + ' 59:59:59'
    table_name = 'app.app_yhzz_umc_algo_pin_interim'
    # table_name = 'app.app_yhzz_umc_algo_gynew_pin_interim'
    # base_file = os.getcwd()
    # base_sql_file = base_file+'/get_base_data.sql'
    # get_lastday_sql_file = base_file+'/get_lastday_data.sql'
    
    # 根据方案档位，获取方案参数
    print(">"*10,"get plan pra")
    a1_threshold = 30
    month_clv_threshold,  year_clv_threshold, channel_max_adjust, channel_preference = get_plan_pra(plan_level = dp)

    print('-----model params-----')
    print('day_ago_1(table_dt):   ', day_ago_1)
    print('day_ago_3(gy_new_dt):  ', day_ago_3)
    print('this_month_end:        ', this_month_end)
    print('dp(table_dp):          ', dp)
    print('user_type_list:        ', user_type_list)
    print('life_type_list:        ', life_type_list)
    print('table_name:            ', table_name)
    print('month_clv_threshold:   ', month_clv_threshold)
    print('year_clv_threshold:    ', year_clv_threshold)
    print('channel_max_adjust:    ', channel_max_adjust)
    print('channel_preference:    ', channel_preference)
    print('a1_threshold:          ', a1_threshold)

    
    # 获取base数据
    print(">"*10,"get base data")
    if user_type_list in ["('gy_new_1')","('gy_new_2')","('gy_new_3')","('gy_new_4')","('gy_new_5')"]:
        base_data_sql_file = 'get_newuser_base_data.sql'
    else:
        base_data_sql_file = 'get_base_data.sql'
        
    base_data = get_base_data(sql_file = base_data_sql_file,
                              part_dt = day_ago_1,
                              part_month= now_month,
                              user_type_list = user_type_list,
                              life_type_list = life_type_list,
                              part_dt_3 = day_ago_3 )

    # 存储参数结果
    base_data = base_data.withColumn('month_clv_threshold', lit(month_clv_threshold))
    base_data = base_data.withColumn('year_clv_threshold', lit(year_clv_threshold))
    base_data = base_data.withColumn('channel_max_adjust', lit(channel_max_adjust))
    base_data = base_data.withColumn('channel_preference', array([lit(x) for x in channel_preference]))
    base_data = base_data.withColumn('a1_threshold', lit(a1_threshold))


    # 进行分配
    print(">"*10,"根据日期、用户类型判断分配方式 进行流量分配") 
    base_data, plan_flag = juge_how_allocation(run_day = day_ago_1,
                                                plan_level = dp,
                                                base_data = base_data,
                                                this_month_end = this_month_end,
                                                priority_type = user_type_list)
    


    
    print(">"*10,"基础校验，保留有效字段")
    # todo 基础数据校验
    base_data = keep_use_cols(base_data)
    print(">"*10,"写表")
    
    data_insert_table(data = base_data,
                      table_name = table_name,
                      table_dt = day_ago_1,
                      tabel_dp = dp)    
    
    # current_time放在程序的末尾
    current_time = time.time()
    print("运行时间为" + str(current_time - old_time) + "s") 
                                                                                                                                                                                                                                                                              
