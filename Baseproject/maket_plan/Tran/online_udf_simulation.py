import time
import datetime
from datetime import timedelta
import math
import sys

from psutil import users
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

# applicationId:spark程序的唯一标识符，其格式取决于调度程序的实现
app_id = spark.sparkContext.applicationId 
print(app_id)
print(spark.version)


# 返回值必须全为Int, 否则类型不对的变量会被设为null
@udf(returnType=MapType(StringType(), IntegerType()))
def get_channel_guaranteed_cnt(com_login_cnt_last_month,
                               free_login_cnt_last_month,
                               strong_ctrl_login_cnt_last_month,
                               normal_ctrl_login_cnt_last_month,
                               no_ctrl_login_cnt_last_month):
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

@udf(returnType=MapType(StringType(), IntegerType()))
def get_result(part_dt,month_clv_threshold,year_clv_threshold,channel_max_adjust,channel_preference,a1_threshold,priority_type,my_hash_code,
                lower_lift_login_cnt, natural_pred_login_cnt_rest,login_cnt_mtd,natural_login_cnt_mtd,no_ctrl_login_cnt_mtd,
                is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl,is_acce_by_no_ctrl,
                com_max_login_count, free_max_login_count, strong_ctrl_max_login_count,normal_ctrl_max_login_count,no_ctrl_max_login_count,
                is_guaranteed_by_com, is_guaranteed_by_free, is_guaranteed_by_strong_ctrl,is_guaranteed_by_normal_ctrl, 
                is_guaranteed_by_no_ctrl,
                com_login_cost, free_login_cost, strong_ctrl_login_cost, normal_login_cost,no_ctrl_login_cost, 
                com_quality_coeff,free_quality_coeff,strong_ctrl_quality_coeff,normal_ctrl_quality_coeff,no_ctrl_quality_coeff,
                com_login_cnt_per_dau, free_login_cnt_per_dau,  strong_ctrl_login_cnt_per_dau, normal_login_cnt_per_dau, no_ctrl_login_cnt_per_dau,
                clv_pred_1m,gmv_year,
                login_cnt_last_month,
                natural_login_cnt_last_month,com_login_cnt_last_month,free_login_cnt_last_month,strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_month):

    channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']

    # 目标
    print('target')
    target_login_days = get_target_login_days(part_dt = part_dt,
                                              lift_login =lower_lift_login_cnt,
                                              natura_login = (natural_login_cnt_last_month-natural_login_cnt_mtd), #用上月自然登录次数替代本月预测v4
                                              current_login = login_cnt_mtd,
                                              no_ctrl_login = (no_ctrl_login_cnt_last_month-no_ctrl_login_cnt_mtd))
    # 渠道上下限 & 参数调整
    channel_max_login_days, channel_min_login_days  = get_channel_max_and_min_func(priority_type,my_hash_code,
                                                                                    com_max_login_count, free_max_login_count, strong_ctrl_max_login_count, normal_ctrl_max_login_count,no_ctrl_max_login_count,
                                                                                    com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month, normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                                                                                    is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl, is_acce_by_no_ctrl,
                                                                                    is_guaranteed_by_com, is_guaranteed_by_free, is_guaranteed_by_strong_ctrl,is_guaranteed_by_normal_ctrl, is_guaranteed_by_no_ctrl,
                                                                                    month_clv_threshold,channel_max_adjust,channel_preference)

    # 渠道成本
    channel_login_costs = get_channel_login_costs(channel_type,
                                                  com_login_cost, free_login_cost, strong_ctrl_login_cost,
                                                  normal_login_cost, no_ctrl_login_cost,
                                                  com_login_cnt_per_dau, free_login_cnt_per_dau,
                                                  strong_ctrl_login_cnt_per_dau, normal_login_cnt_per_dau,
                                                  no_ctrl_login_cnt_per_dau,
                                                  com_quality_coeff, free_quality_coeff, strong_ctrl_quality_coeff,
                                                  normal_ctrl_quality_coeff, no_ctrl_quality_coeff)
    
    # 用户价值
    clv = get_user_clv(clv_pred_1m)

    # 贪心优化算法输出贪心解
    greedy_result = greedy_solver(target_login_days,
                                channel_max_login_days,
                                channel_min_login_days,
                                channel_login_costs,
                                clv)

    # 根据用户类型修正渠道分配方案
    gmv_year = gmv_year if gmv_year else 0
    user_label = priority_type
    
    final_result = check_user_plan(greedy_result, user_label, target_login_days, year_clv_threshold, gmv_year,clv,
                                   month_clv_threshold, a1_threshold,channel_max_adjust, 
                                   com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month,
                                   normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                                   login_cnt_last_month)

    return final_result

def get_target_login_days(part_dt,lift_login, natura_login, current_login,no_ctrl_login):
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

def get_channel_max_and_min_func(priority_type,my_hash_code,
                    com_max_login_count, free_max_login_count, strong_ctrl_max_login_count, normal_ctrl_max_login_count,no_ctrl_max_login_count,
                    com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month, normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                    is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl, is_acce_by_no_ctrl,
                    is_guaranteed_by_com, is_guaranteed_by_free, is_guaranteed_by_strong_ctrl,is_guaranteed_by_normal_ctrl, is_guaranteed_by_no_ctrl,
                    month_clv_threshold,channel_max_adjust,channel_preference):
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
    # 参数2 channel_max_adjust
    if channel_max_adjust == 999:
        channel_max_login_days['com'] = math.ceil(com_max_login_count) if is_acce_by_com >0 else channel_max_login_days['com'] 
        channel_max_login_days['free'] = math.ceil(free_max_login_count) if is_acce_by_free >0 else channel_max_login_days['free'] 
        
        channel_max_login_days['strong_ctrl'] = math.ceil(strong_ctrl_max_login_count) if is_acce_by_strong_ctrl >0 else channel_max_login_days['strong_ctrl'] 
        
        channel_max_login_days['normal_ctrl'] = math.ceil(normal_ctrl_max_login_count) if is_acce_by_normal_ctrl >0 else channel_max_login_days['normal_ctrl'] 
        
        # channel_max_login_days['no_ctrl'] = math.ceil(no_ctrl_max_login_count) if is_acce_by_no_ctrl >0 else channel_max_login_days['no_ctrl'] 
    else:
        channel_max_login_days['com'] = math.ceil(channel_max_adjust * com_login_cnt_last_month) # 上月干预次数的标签大于可达标签
        channel_max_login_days['free'] = math.ceil(channel_max_adjust * free_login_cnt_last_month)
        channel_max_login_days['strong_ctrl'] = math.ceil(channel_max_adjust * strong_ctrl_login_cnt_last_month)
        channel_max_login_days['normal_ctrl'] = math.ceil(channel_max_adjust * normal_ctrl_login_cnt_last_month)
    
    channel_max_login_days['no_ctrl'] = math.ceil(1.0 * no_ctrl_login_cnt_last_month)
    # TODO 渠道上限的修正可以写在贪心算法的求解里
    

    # 参数3 channel_preference
    # channel_login_costs['com'] = float(channel_login_costs['com'] )/ float(channel_preference[0])
    # channel_login_costs['free'] = float(channel_login_costs['free'] )/ float( channel_preference[1])
    # channel_login_costs['strong_ctrl'] =  float(channel_login_costs['strong_ctrl']) / float( channel_preference[2])
    # channel_login_costs['normal_ctrl'] = float(channel_login_costs['normal_ctrl']) / float(channel_preference[3])
    # channel_login_costs['no_ctrl'] = float(channel_login_costs['com']) / float( channel_preference[4])

    # 获得channel_min_login_days
    is_guaranteed_by_com = is_guaranteed_by_com if is_guaranteed_by_com else 1
    is_guaranteed_by_free = is_guaranteed_by_free if is_guaranteed_by_free else 1
    is_guaranteed_by_strong_ctrl = is_guaranteed_by_strong_ctrl if is_guaranteed_by_strong_ctrl else 0
    is_guaranteed_by_normal_ctrl = is_guaranteed_by_normal_ctrl if is_guaranteed_by_normal_ctrl else 0
    is_guaranteed_by_no_ctrl = is_guaranteed_by_no_ctrl if is_guaranteed_by_no_ctrl else 0

    channel_min_login_days = {'com' : is_guaranteed_by_com,
                              'free' : is_guaranteed_by_free,
                              'strong_ctrl' : is_guaranteed_by_strong_ctrl,
                              'normal_ctrl' : is_guaranteed_by_normal_ctrl,
                              'no_ctrl' : is_guaranteed_by_no_ctrl}
    
    # 根据业务逻辑调整渠道上限
    # TODO 商业化
    com_adjust_pra = 1.2
    # strong_ctrl_adjust_pra = 1.25
    hash_precent = 28
    msg_add = 1
    
    if priority_type== 'A1':
        # 京东可控置0 
        channel_max_login_days['normal_ctrl']  = 0
        channel_min_login_days['normal_ctrl']  = 0
        # 商业化渠道补量
        # 
        adjust_day = com_adjust_pra * com_login_cnt_last_month * 100
        adjust_day_base ,  adjust_day_hash_add= adjust_day//100,int(adjust_day%100)
        channel_max_login_days['com'] = min(adjust_day_base,channel_max_login_days['com']) # V2
        if (my_hash_code<= hash_precent)<= adjust_day_hash_add:
            channel_min_login_days['com'] += 1
            
        # 免费渠道补量
        channel_min_login_days['free'] = channel_min_login_days['free'] + 1 
        # 短信渠道补量
        # channel_min_login_days['strong_ctrl'] = math.ceil( strong_ctrl_adjust_pra * channel_min_login_days['strong_ctrl'])
        if (my_hash_code<= hash_precent):
            channel_min_login_days['strong_ctrl'] += msg_add
        else:
            pass
             
        
    # 最大最小修正
    # TODO verify this
    for c_name in channel_max_login_days:
        channel_max_day,channel_min_day = channel_max_login_days[c_name] , channel_min_login_days[c_name]
        channel_max_login_days[c_name], channel_min_login_days[c_name] = max([channel_max_day, channel_min_day]), min([channel_max_day,channel_min_day])

    return channel_max_login_days, channel_min_login_days

def get_channel_login_costs(channel_type,
                            com_login_cost,free_login_cost,strong_ctrl_login_cost,normal_login_cost,no_ctrl_login_cost,
                            com_login_cnt_per_dau,free_login_cnt_per_dau,strong_ctrl_login_cnt_per_dau,normal_login_cnt_per_dau,no_ctrl_login_cnt_per_dau,
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
        normal_login_cnt_per_dau (_type_): _description_
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
    channel_pre_dau = [com_login_cnt_per_dau,free_login_cnt_per_dau,strong_ctrl_login_cnt_per_dau,normal_login_cnt_per_dau,no_ctrl_login_cnt_per_dau,]
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

def get_user_clv(clv_pred_1m):
    """_summary_

    Args:
        clv_pred_1m (_type_): _description_

    Returns:
        _type_: _description_
    """
    return max(clv_pred_1m,0)



def greedy_solver(target_login_days, channel_max_login_days, channel_min_login_days, login_costs, clv):
    """

    :param target_login_days:
    :param channel_max_login_days:
    :param channel_min_login_days:
    :param login_costs:
    :param clv:
    :return:
    """
    # channel_type = ['com','free','strong_ctrl','normal_ctrl','no_ctrl']
    greedy_result = {'com': 0, 'free': 0, 'strong_ctrl': 0, 'normal_ctrl': 0, 'no_ctrl': 0}

    login_costs_dict = login_costs
    sorted_costs = sorted(login_costs_dict.items(), key=lambda kv: (kv[1], kv[0]))

    for i in range(len(sorted_costs)): 
        
        used_day = 0
        index = sorted_costs[i][0]
        if index == 'no_ctrl':
            continue

        channel_max_day = math.ceil(channel_max_login_days[index])
        channel_min_day = math.ceil(channel_min_login_days[index])

        if target_login_days >= channel_max_day:
            used_day = channel_max_day

        elif (target_login_days < channel_max_day) :
            used_day = max(target_login_days, channel_min_day)

        target_login_days -= used_day
        target_login_days = max(0, target_login_days)

        greedy_result[index] = used_day

    return greedy_result

def check_user_plan(greedy_result, user_label, target_login_days, year_clv_threshold, gmv_year,clv,
                    month_clv_threshold, a1_threshold,channel_max_adjust,
                    com_login_cnt_last_month, free_login_cnt_last_month, strong_ctrl_login_cnt_last_month,
                    normal_ctrl_login_cnt_last_month, no_ctrl_login_cnt_last_month,
                    login_cnt_last_month):
    """根据用户逻辑进行方案调整

    Args:
        greedy_result (_type_): _description_
        user_label (_type_): _description_
        target_login_days (_type_): _description_
        year_clv_threshold (_type_): _description_
        annual_clv_1st (_type_): _description_
        clv (_type_): _description_
        month_clv_threshold (_type_): _description_
        a1_threshold (_type_): _description_
        channel_max_adjust (_type_): _description_
        com_login_cnt_last_month (_type_): _description_
        free_login_cnt_last_month (_type_): _description_
        strong_ctrl_login_cnt_last_month (_type_): _description_
        normal_ctrl_login_cnt_last_month (_type_): _description_
        no_ctrl_login_cnt_last_month (_type_): _description_
        natural_pred_login_cnt_n30d (_type_): _description_

    Returns:
        _type_: _description_
    """
    # channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']
    # TODO 上月登端次数小于某阈值，天宫搜客不投 P0
   
    final_result = greedy_result
    year_clv_threshold = 600.0
    login_cnt_last_month = login_cnt_last_month if login_cnt_last_month else 5
    a1_threshold = 10

    if user_label == 'A1':
        pass
        # if login_cnt_last_month< a1_threshold:
        #     final_result['normal_ctrl'] = math.ceil(normal_ctrl_login_cnt_last_month)
        # else:
        #     final_result['normal_ctrl'] = 0
        
        # final_result['com'] = min(int(1.2 * com_login_cnt_last_month),final_result['com'] )

    elif user_label == 'A2':
        if clv <= month_clv_threshold:
            final_result = get_base_plan(greedy_result, com_login_cnt_last_month,free_login_cnt_last_month,
                                         strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_month)
        else:
            pass

    elif user_label == 'B':
        final_result = get_base_plan(greedy_result, com_login_cnt_last_month,free_login_cnt_last_month,
                                         strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_month)

    elif user_label == 'C':
        # TODO c人群修正
        if gmv_year <= year_clv_threshold:
            # 兜底
            final_result = get_base_plan(greedy_result, com_login_cnt_last_month,free_login_cnt_last_month,
                                         strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_month)

        else:
            
            final_result['normal_ctrl'] = min(20, 1.5 * normal_ctrl_login_cnt_last_month,final_result['normal_ctrl'])

    # 结果最终修正 不小于上月用户次数
    final_result['com'] = math.ceil(max(final_result['com'],com_login_cnt_last_month))
    final_result['free'] = math.ceil(max(final_result['free'],free_login_cnt_last_month))
    final_result['strong_ctrl'] = math.ceil(max(final_result['strong_ctrl'],strong_ctrl_login_cnt_last_month))
    final_result['normal_ctrl'] = math.ceil(max(final_result['normal_ctrl'],normal_ctrl_login_cnt_last_month))
    final_result['no_ctrl'] = math.ceil(max(final_result['no_ctrl'],normal_ctrl_login_cnt_last_month))

    re = sum(list(final_result.values()))
    target_flag = 1 if re >= target_login_days else 0
    final_result['flag'] = target_flag

    return final_result

def get_base_plan(result, com_login_cnt_last_month,free_login_cnt_last_month,
                  strong_ctrl_login_cnt_last_month,normal_ctrl_login_cnt_last_month,no_ctrl_login_cnt_last_mont):
    """用户兜底方案

    Args:
        result (_type_): _description_
        com_login_cnt_last_month (_type_): _description_
        free_login_cnt_last_month (_type_): _description_
        strong_ctrl_login_cnt_last_month (_type_): _description_
        normal_ctrl_login_cnt_last_month (_type_): _description_
        no_ctrl_login_cnt_last_mont (_type_): _description_

    Returns:
        dict: 用户各渠道兜底干预次数
    """
    # greedy_result = {'com': 0, 'free': 0, 'strong_ctrl': 0, 'normal_ctrl': 0, 'no_ctrl': 0}
    base_result = result
    base_result['com'] = math.ceil(com_login_cnt_last_month) # 向上取整
    base_result['free'] = math.ceil(free_login_cnt_last_month) # 向上取整
    base_result['strong_ctrl'] = math.ceil(strong_ctrl_login_cnt_last_month) # 向上取整
    base_result['normal_ctrl'] = math.ceil(normal_ctrl_login_cnt_last_month) # 向上取整
    base_result['no_ctrl'] = math.ceil(no_ctrl_login_cnt_last_mont) # 向上取整

    return base_result


'''
def get_channel_max_login_days(channel_type,priority_type,is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl, is_acce_by_normal_ctrl, is_acce_by_no_ctrl,
                                com_max_login_count, free_max_login_count, strong_ctrl_max_login_count, normal_ctrl_max_login_count,no_ctrl_max_login_count,
                                com_base_cnt):
    """
    获取各渠道在给定时间窗口内的引流能力上限,同时结合用户可达标签进行修正
    :param channel_type: 渠道类型
    :param is_acce_by_com: 商业化是否可达
    :param is_acce_by_free: 免费
    :param is_acce_by_strong_ctrl: 强控
    :param is_acce_by_normal_ctrl: 可控
    :param is_acce_by_no_ctrl: 不可控
    :param com_max_login_count: 商业化渠道干预上限
    :param free_max_login_count:
    :param strong_ctrl_max_login_count:
    :param normal_ctrl_max_login_count:
    :param no_ctrl_max_login_count:
    :return:
   """
    # TODO 是否需要加一下判断空值的函数

    max_login_days = [com_max_login_count, free_max_login_count, strong_ctrl_max_login_count,normal_ctrl_max_login_count, no_ctrl_max_login_count]
    acce_flag = [is_acce_by_com, is_acce_by_free, is_acce_by_strong_ctrl,is_acce_by_normal_ctrl, is_acce_by_no_ctrl]

    for i in range(len(acce_flag)):
        if acce_flag[i] == 0:
            max_login_days[i] = 0
    

    return dict(zip(channel_type, max_login_days))


def get_channel_min_login_days(channel_type,
                               is_guaranteed_by_com, is_guaranteed_by_free, is_guaranteed_by_strong_ctrl,is_guaranteed_by_normal_ctrl, is_guaranteed_by_no_ctrl):
    """
    :param channel_type:
    :param is_guaranteed_by_com:
    :param is_guaranteed_by_free:
    :param is_guaranteed_by_strong_ctrl:
    :param is_guaranteed_by_normal_ctrl:
    :param is_guaranteed_by_no_ctrl:
    :return:
    """
    is_guaranteed_by_com = is_guaranteed_by_com if is_guaranteed_by_com else 1
    is_guaranteed_by_free = is_guaranteed_by_free if is_guaranteed_by_free else 1
    is_guaranteed_by_strong_ctrl = is_guaranteed_by_strong_ctrl if is_guaranteed_by_strong_ctrl else 0
    is_guaranteed_by_normal_ctrl = is_guaranteed_by_normal_ctrl if is_guaranteed_by_normal_ctrl else 0
    is_guaranteed_by_no_ctrl = is_guaranteed_by_no_ctrl if is_guaranteed_by_no_ctrl else 0

    min_login_days = [is_guaranteed_by_com, is_guaranteed_by_free, is_guaranteed_by_strong_ctrl,is_guaranteed_by_normal_ctrl, is_guaranteed_by_no_ctrl]

    return  dict(zip(channel_type, min_login_days))

def get_clv_plan(greedy_result, year_clv_threshold, annual_clv_1st):
    annual_clv_1st = annual_clv_1st if annual_clv_1st else 0
    clv_result = greedy_result
    # TODO verify threshold
    # if annual_clv_1st>year_clv_threshold:
    if annual_clv_1st not in ['0', '1']:
        return clv_result

    else:
        cnt_tmp = clv_result['normal_ctrl']
        clv_result['normal_ctrl'] = 0
        clv_result['com'] += cnt_tmp

    return clv_result


def get_user_label(user_life_cycle_type_1st,model_a_1st,model_b_1st,goal_group_1st,annual_clv_1st,area_b_1st):
    
    """
    根据用户标签,选择方案输出形式
    :param user_life_cycle_type_1st:
    :param model_a_1st:
    :param model_b_1st:
    :param goal_group_1st:
    :param annual_clv_1st:
    :param area_b_1st:
    :param clv:
    :return:
    """

    # user_label = 'a_high_tran','a_low_tran','b','c_high_clv','c_low_clv','others'
    user_label = 'others'

    if (model_a_1st in ['41', '42', '43', '5']) and (model_b_1st in ['3', '4', '5']) and (
            user_life_cycle_type_1st in ['成长期', '成熟期']) and (goal_group_1st != '未知'):
        if (model_a_1st == 5) or (area_b_1st == 0):
            user_label = 'a_high_tran'
        else:
            user_label = 'a_low_tran'

    return user_label
'''


if __name__ == '__main__':
    today_str =  sys.argv[1]
    now = datetime.datetime.strptime(today_str, '%Y-%m-%d')
    day_ago_1 =  datetime.datetime.strptime(today_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    
    this_month_end = datetime.datetime.strftime(datetime.datetime(now.year, now.month + 1, 1) - timedelta(seconds=1),
                                                '%Y-%m-%d %H:%M:%S')
    print("day_ago_1",day_ago_1)
    print("this_month_end",this_month_end)

    base_data_sql = """
    SELECT
        user.dt,
        user.user_log_acct,
        user.my_hash_code,
        --格子粒度
        user.user_life_cycle_type_1st,
        user.model_a_1st,
        user.model_b_1st,
        user.goal_group_1st,
        user.annual_clv_1st,
        user.grid_name_1st,
        user.area_b_1st,

        --格子粒度2
        user.user_life_cycle_type,
        user.model_a,
        user.model_b,
        user.model_c,
        user.model_l,
        user.priority_type,
        
        -- 目标登录次数
        COALESCE(grid.lower_lift_login_cnt,0) as lower_lift_login_cnt,
        COALESCE(user.natural_pred_login_cnt_rest,0) as natural_pred_login_cnt_rest,
        COALESCE(user.natural_pred_login_cnt_n30d,0) as natural_pred_login_cnt_n30d,


        --当月引流登端次数
        COALESCE(user.login_cnt_mtd,0) as login_cnt_mtd,
        user.natural_login_cnt_mtd,
        user.com_login_cnt_mtd,
        user.free_login_cnt_mtd,
        user.strong_ctrl_login_cnt_mtd,
        user.normal_ctrl_login_cnt_mtd,
        user.no_ctrl_login_cnt_mtd,

        --渠道是否可达
        user.is_acce_by_com,
        user.is_acce_by_free,
        user.is_acce_by_strong_ctrl,
        user.is_acce_by_normal_ctrl,
        user.is_acce_by_no_ctrl,

        --clv
        user.clv_pred_1m,
        user.clv_mtd,

        --上月人均干预次数
        COALESCE(login_cnt_last_month,0)         as login_cnt_last_month,
        COALESCE(natural_login_cnt_last_month,0) as natural_login_cnt_last_month,
        COALESCE(com_login_cnt_last_month,0) as com_login_cnt_last_month,
        COALESCE(free_login_cnt_last_month,0) as free_login_cnt_last_month,
        COALESCE(strong_ctrl_login_cnt_last_month,0) as strong_ctrl_login_cnt_last_month,
        COALESCE(normal_ctrl_login_cnt_last_month,0) as normal_ctrl_login_cnt_last_month,
        COALESCE(no_ctrl_login_cnt_last_month,0) as no_ctrl_login_cnt_last_month,
        
        --gmv
        cast(COALESCE(gmv_year,0.0) as double)  as gmv_year,
        cast(COALESCE(last_year_gmv,0.0) as double) as last_year_gmv,
                
        --渠道上限
        COALESCE(grid.com_max_login_count,2) as com_max_login_count,
        COALESCE(grid.free_max_login_count,2) as free_max_login_count,
        COALESCE(grid.strong_ctrl_max_login_count,0) as strong_ctrl_max_login_count,
        COALESCE(grid.normal_ctrl_max_login_count,0) as normal_ctrl_max_login_count,
        COALESCE(grid.no_ctrl_max_login_count,0) as no_ctrl_max_login_count,

        --渠道成本
        COALESCE(grid.com_login_cost,-3) as com_login_cost,
        COALESCE(grid.free_login_cost,0) as free_login_cost,
        COALESCE(grid.no_ctrl_login_cost,0.9) as no_ctrl_login_cost,
        COALESCE(grid.strong_ctrl_login_cost,0.8) as strong_ctrl_login_cost,
        COALESCE(grid.normal_login_cost,0.6) as normal_login_cost,

        --渠道成本质量因子
        com_quality_coeff,
        free_quality_coeff,
        strong_ctrl_quality_coeff,
        normal_ctrl_quality_coeff,
        no_ctrl_quality_coeff,


        --渠道dau映射
        COALESCE(grid.com_login_cnt_per_dau,0.5)            as com_login_cnt_per_dau,
        COALESCE(grid.free_login_cnt_per_dau,0.5)           as free_login_cnt_per_dau,
        COALESCE(grid.no_ctrl_login_cnt_per_dau,0.5)        as no_ctrl_login_cnt_per_dau,
        COALESCE(grid.strong_ctrl_login_cnt_per_dau,0.5)    as strong_ctrl_login_cnt_per_dau,
        COALESCE(grid.normal_login_cnt_per_dau,0.5)         as normal_login_cnt_per_dau,
        grid.total_login_dau_ratio,

        -- 格子用户数& 渠道月均引流 for 保量
        case when grid.user_cnt <-100 then 1 else grid.user_cnt end as user_cnt,
        case when grid.com_login_cnt_per_month <-100 then 1 else grid.com_login_cnt_per_month end as com_login_cnt_per_month,
        case when grid.free_login_cnt_per_month <-100 then 1 else grid.free_login_cnt_per_month end as free_login_cnt_per_month,
        case when grid.no_ctrl_login_cnt_per_month <-100 then 1 else grid.no_ctrl_login_cnt_per_month end as no_ctrl_login_cnt_per_month,
        case when grid.strong_ctrl_login_cnt_per_month <-100 then 1 else grid.strong_ctrl_login_cnt_per_month end as strong_ctrl_login_cnt_per_month,
        case when grid.normal_ctrl_login_cnt_per_month <-100 then 1 else grid.normal_ctrl_login_cnt_per_month end as normal_ctrl_login_cnt_per_month,

        -- 上月人均引流次数（按格子统计）
        case when grid.com_base_cnt         <-100 then 0 else grid.com_base_cnt end as         com_base_cnt,
        case when grid.free_base_cnt        <-100 then 0 else grid.free_base_cnt end as        free_base_cnt,
        case when grid.no_ctrl_base_cnt     <-100 then 0 else grid.no_ctrl_base_cnt end as     no_ctrl_base_cnt,
        case when grid.strong_ctrl_base_cnt  <-100 then 0 else grid.strong_ctrl_base_cnt end as strong_ctrl_base_cnt,
        case when grid.normal_ctrl_base_cnt <-100 then 0 else grid.normal_ctrl_base_cnt end as normal_ctrl_base_cnt

    FROM
    (
    SELECT
        dt,
        user_log_acct,
        hash_sub(concat(lower(trim(user_log_acct)),'{part_dt}')) as my_hash_code,
        --格子粒度
        user_life_cycle_type_1st,
        model_a_1st,
        model_b_1st,
        goal_group_1st,
        case when annual_clv_1st is null then '0' else annual_clv_1st end as annual_clv_1st,
        grid_name_1st,
        area_b_1st,

        user_life_cycle_type,
        model_a,
        model_b,
        model_c,
        model_l,
        priority_type,
        
        -- 自然登录
        case when natural_pred_login_cnt_rest<-100 then 0
             else natural_pred_login_cnt_rest end as natural_pred_login_cnt_rest,
        case when natural_pred_login_cnt_n30d<-100 then 0
             else natural_pred_login_cnt_n30d end as natural_pred_login_cnt_n30d,


        
        
        --当月引流登端次数
        case when login_cnt_mtd<-100 then 0
             else login_cnt_mtd end as login_cnt_mtd,
        case when natural_login_cnt_mtd <-100 then 0 else natural_login_cnt_mtd end as natural_login_cnt_mtd,
        case when com_login_cnt_mtd <-100 then 0 else com_login_cnt_mtd end as com_login_cnt_mtd,
        case when free_login_cnt_mtd <-100 then 0 else free_login_cnt_mtd end as free_login_cnt_mtd,
        case when strong_ctrl_login_cnt_mtd <-100 then 0 else strong_ctrl_login_cnt_mtd end as strong_ctrl_login_cnt_mtd,
        case when normal_ctrl_login_cnt_mtd <-100 then 0 else normal_ctrl_login_cnt_mtd end as normal_ctrl_login_cnt_mtd,
        case when no_ctrl_login_cnt_mtd <-100 then 0 else no_ctrl_login_cnt_mtd end as no_ctrl_login_cnt_mtd,
        
        --渠道是否可达
        case when is_acce_by_com <-100 then 1 else is_acce_by_com end as is_acce_by_com,
        case when is_acce_by_free <-100 then 1 else is_acce_by_free end as is_acce_by_free,
        case when is_acce_by_strong_ctrl <-100 then 1 else is_acce_by_strong_ctrl end as is_acce_by_strong_ctrl,
        case when is_acce_by_normal_ctrl <-100 then 1 else is_acce_by_normal_ctrl end as is_acce_by_normal_ctrl,
        case when is_acce_by_no_ctrl <-100 then 1 else is_acce_by_no_ctrl end as is_acce_by_no_ctrl,
        
        --clv
        case when clv_pred_1m <-100 then 0 else clv_pred_1m end as clv_pred_1m, --0
        case when clv_mtd <-100 then 0 else clv_mtd end as clv_mtd, --0

        --上月人均干预次数
        case when login_cnt_last_month <-100 then 0 else login_cnt_last_month end                         as login_cnt_last_month,
        case when natural_login_cnt_last_month <-100 then 0 else natural_login_cnt_last_month end         as natural_login_cnt_last_month,
        case when com_login_cnt_last_month <-100 then 0 else com_login_cnt_last_month end                 as com_login_cnt_last_month,
        case when free_login_cnt_last_month <-100 then 0 else free_login_cnt_last_month end               as free_login_cnt_last_month,
        case when strong_ctrl_login_cnt_last_month<-100 then 0 else strong_ctrl_login_cnt_last_month end  as strong_ctrl_login_cnt_last_month,
        case when normal_ctrl_login_cnt_last_month<-100 then 0 else normal_ctrl_login_cnt_last_month end  as normal_ctrl_login_cnt_last_month,
        case when no_ctrl_login_cnt_last_month<-100 then 0 else no_ctrl_login_cnt_last_month end          as no_ctrl_login_cnt_last_month,
        case when clv_year<-100 then 0 else clv_year           end as gmv_year,
        case when last_year_gmv<-100 then 0 else last_year_gmv end as last_year_gmv
        
    FROM
    app.app_yhzz_umc_unit_user
    WHERE
    dt = '{part_dt}'
    and priority_type in('A1','A2')
    )user

    JOIN

    (
    SELECT
        grid_name_1st,
        
        --阶跃下限
        max(case when lower_lift_login_cnt <-100 then 0 else lower_lift_login_cnt end) as lower_lift_login_cnt,
             
        
        --渠道上限
        max(case when com_max_login_cnt <-100 then 2 else com_max_login_cnt end )  as com_max_login_count,
        max(case when free_max_login_cnt <-100 then 2 else free_max_login_cnt end ) as free_max_login_count,
        max(case when strong_ctrl_max_login_count <-100 then 0 else strong_ctrl_max_login_count end ) as strong_ctrl_max_login_count,
        max(case when normal_ctrl_max_login_count <-100 then 0 else normal_ctrl_max_login_count end ) as normal_ctrl_max_login_count,
        max(case when no_ctrl_max_login_count <-100 then 0 else no_ctrl_max_login_count end ) as no_ctrl_max_login_count,
        
        --渠道成本
        max(case when com_login_cost <-100 then -3 else (-1 * com_login_cost) end ) as  com_login_cost,
        max(case when free_login_cost <-100 then 0 else free_login_cost end ) as free_login_cost,
        max(case when no_ctrl_login_cost <-100 then 0.8 else no_ctrl_login_cost end ) as no_ctrl_login_cost,
        max(case when strong_ctrl_login_cost <-100 then 0.6 else strong_ctrl_login_cost end ) as strong_ctrl_login_cost,
        max(case when normal_login_cost <-100 then 0.9 else normal_login_cost end ) as normal_login_cost,

        --渠道成本质量因子
        max(case when com_quality_coeff <-100 then 0.28 else com_quality_coeff end ) as com_quality_coeff,
        max(case when free_quality_coeff <-100 then 0.64 else free_quality_coeff end ) as free_quality_coeff,
        max(case when strong_ctrl_quality_coeff <-100 then 2.2 else strong_ctrl_quality_coeff end ) as strong_ctrl_quality_coeff,
        max(case when normal_ctrl_quality_coeff <-100 then 0.25 else normal_ctrl_quality_coeff end ) as normal_ctrl_quality_coeff,
        max(case when no_ctrl_quality_coeff <-100 then 0.88 else no_ctrl_quality_coeff end ) as no_ctrl_quality_coeff,
        
        --渠道dau映射
        max(case when com_login_cnt_per_dau <-100 then 0.478 else com_login_cnt_per_dau end ) as com_login_cnt_per_dau,
        max(case when free_login_cnt_per_dau <-100 then 0.462 else free_login_cnt_per_dau end ) as free_login_cnt_per_dau,
        max(case when no_ctrl_login_cnt_per_dau <-100 then 0.434 else no_ctrl_login_cnt_per_dau end ) as no_ctrl_login_cnt_per_dau,
        max(case when strong_ctrl_login_cnt_per_dau <-100 then 0.596 else strong_ctrl_login_cnt_per_dau end ) as strong_ctrl_login_cnt_per_dau,
        max(case when normal_login_cnt_per_dau <-100 then 0.507 else normal_login_cnt_per_dau end ) as normal_login_cnt_per_dau,
         --dau映射
        max(case when total_login_dau_ratio <-100 then 0.8 else total_login_dau_ratio end) as total_login_dau_ratio,

        -- 格子用户数& 渠道月均引流 for 保量
        max(user_cnt) as user_cnt,
        max(com_login_cnt_per_month) as com_login_cnt_per_month,
        max(free_login_cnt_per_month) as free_login_cnt_per_month,
        max(no_ctrl_login_cnt_per_month) as no_ctrl_login_cnt_per_month,
        max(strong_ctrl_login_cnt_per_month) as strong_ctrl_login_cnt_per_month,
        max(normal_ctrl_login_cnt_per_month) as normal_ctrl_login_cnt_per_month,
        max(cast(com_login_cnt_per_month as  double)/user_cnt)         as com_base_cnt,
        max(cast(free_login_cnt_per_month as  double)/user_cnt)        as free_base_cnt,
        max(cast(no_ctrl_login_cnt_per_month as  double)/user_cnt)     as no_ctrl_base_cnt,
        max(cast(strong_ctrl_login_cnt_per_month as  double)/user_cnt) as strong_ctrl_base_cnt,
        max(cast(normal_ctrl_login_cnt_per_month as  double)/user_cnt) as normal_ctrl_base_cnt

        
    FROM
        app.app_yhzz_umc_unit_grid
    WHERE
        dt = '{part_dt}'
        AND grid_name_1st is not null
        AND is_grid_valid =1 
        group by
        grid_name_1st
    )grid on user.grid_name_1st = grid.grid_name_1st

    """.format(part_dt = day_ago_1)
    print(base_data_sql)
    spark.sql("""ADD JAR hdfs://ns1009/user/mart_jypt/mart_jypt_usr_grow/liuyang266/hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar""")
    spark.sql("""CREATE TEMPORARY FUNCTION hash_sub AS 'com.jd.bdptools.ly.HashMonthly'""")
    
    base_data = spark.sql(base_data_sql)
    
    base_data.cache()
    # base_data.fillna(0)
    print('base_data1', base_data.columns)
    print(base_data.dtypes)
    # print("base_data cnt",base_data.count())
    
    # 相关参数
    dp = 'medium'
    a1_threshold = 30
    if dp == 'max':
        month_clv_threshold = 50  # 月度clv阈值
        #     year_clv_threshold = 500  # 年度clv阈值
        year_clv_threshold = '1'  # 年度clv等级
        channel_max_adjust = 999  # 渠道上限调节, 999表示取上限，1.5表示均值的1.5倍，1.2表示均值的1.2倍，等等
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  # 渠道倾向 channel_type = ['com','free', 'strong_ctrl', 'normal_ctrl', 'no_ctrl']
    elif dp == 'high':
        month_clv_threshold = 50  
        #     year_clv_threshold = 500 
        year_clv_threshold = '1'  
        channel_max_adjust = 1.5  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  
    elif dp == 'medium':
        month_clv_threshold = 50 
        #     year_clv_threshold = 500  
        year_clv_threshold = '1' 
        channel_max_adjust = 1.2  
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  
    else:
        dp = 'low'
        month_clv_threshold = 50  
        #     year_clv_threshold = 500  # 年度clv阈值
        year_clv_threshold = '1'  
        channel_max_adjust = 1.0 
        channel_preference = [1.0, 1.0, 1.0, 1.0,
                              1.0]  

    print(dp, month_clv_threshold, year_clv_threshold, channel_max_adjust, channel_preference)

    base_data = base_data.withColumn('month_clv_threshold', lit(month_clv_threshold))
    base_data = base_data.withColumn('year_clv_threshold', lit(year_clv_threshold))
    base_data = base_data.withColumn('channel_max_adjust', lit(channel_max_adjust))
    base_data = base_data.withColumn('channel_preference', array([lit(x) for x in channel_preference]))
    base_data = base_data.withColumn('a1_threshold', lit(a1_threshold))

    # 第一层分配(保量准则)
    print("第一层分配")
    base_data = base_data.withColumn('guaranteed',get_channel_guaranteed_cnt('com_login_cnt_last_month',
                                                                             'free_login_cnt_last_month',
                                                                             'strong_ctrl_login_cnt_last_month',
                                                                             'normal_ctrl_login_cnt_last_month',
                                                                             'no_ctrl_login_cnt_last_month'))
    # 引流次数
    base_data = base_data.withColumn('is_guaranteed_by_com', base_data["guaranteed"].getItem("com"))
    base_data = base_data.withColumn('is_guaranteed_by_free', base_data["guaranteed"].getItem("free"))
    base_data = base_data.withColumn('is_guaranteed_by_strong_ctrl', base_data["guaranteed"].getItem("strong_ctrl"))
    base_data = base_data.withColumn('is_guaranteed_by_normal_ctrl', base_data["guaranteed"].getItem("normal_ctrl"))
    base_data = base_data.withColumn('is_guaranteed_by_no_ctrl', base_data["guaranteed"].getItem("no_ctrl"))
    print('base_data2', base_data.columns)

    base_data.cache()


    # 第二层分配(动态分配)
    print("第二层分配")
    base_data = base_data.withColumn('result',get_result('dt','month_clv_threshold','year_clv_threshold','channel_max_adjust',
                                                         'channel_preference','a1_threshold','priority_type','my_hash_code',
                                                         'lower_lift_login_cnt','natural_pred_login_cnt_rest','login_cnt_mtd','no_ctrl_login_cnt_mtd',
                                                         'natural_login_cnt_mtd',
                                                         'is_acce_by_com','is_acce_by_free','is_acce_by_strong_ctrl',
                                                         'is_acce_by_normal_ctrl','is_acce_by_no_ctrl',
                                                         'com_max_login_count','free_max_login_count','strong_ctrl_max_login_count',
                                                         'normal_ctrl_max_login_count','no_ctrl_max_login_count',
                                                         'is_guaranteed_by_com','is_guaranteed_by_free','is_guaranteed_by_strong_ctrl',
                                                         'is_guaranteed_by_normal_ctrl','is_guaranteed_by_no_ctrl',
                                                         'com_login_cost','free_login_cost','strong_ctrl_login_cost','normal_login_cost',
                                                         'no_ctrl_login_cost',
                                                         'com_quality_coeff','free_quality_coeff','strong_ctrl_quality_coeff',
                                                         'normal_ctrl_quality_coeff','no_ctrl_quality_coeff',
                                                         'com_login_cnt_per_dau','free_login_cnt_per_dau',
                                                         'strong_ctrl_login_cnt_per_dau','normal_login_cnt_per_dau',
                                                         'no_ctrl_login_cnt_per_dau',
                                                         'clv_pred_1m','gmv_year',
                                                         'natural_login_cnt_last_month','login_cnt_last_month','com_login_cnt_last_month','free_login_cnt_last_month','strong_ctrl_login_cnt_last_month','normal_ctrl_login_cnt_last_month','no_ctrl_login_cnt_last_month'))

    # 引流次数
    base_data = base_data.withColumn('com_login_cnt', base_data["result"].getItem("com"))
    base_data = base_data.withColumn('free_login_cnt', base_data["result"].getItem("free"))
    base_data = base_data.withColumn('strong_ctrl_login_cnt', base_data["result"].getItem("strong_ctrl"))
    base_data = base_data.withColumn('normal_ctrl_login_cnt', base_data["result"].getItem("normal_ctrl"))
    base_data = base_data.withColumn('no_ctrl_login_cnt', base_data["result"].getItem("no_ctrl"))
    # 成功标志
    base_data = base_data.withColumn('is_solvable', base_data["result"].getItem("flag"))

    # TODO 优先级 P1
    base_data = base_data.withColumn('priority_level', lit(50))

    # DAU
    # base_data = base_data.withColumn("natural_pred_login_cnt_rest")
    # TODO DAU修正 DAU最大为30
    base_data = base_data.withColumn("pred_com_dau", col("com_login_cnt") * col("com_login_cnt_per_dau"))
    base_data = base_data.withColumn("pred_free_dau", col("free_login_cnt") * col("free_login_cnt_per_dau"))
    base_data = base_data.withColumn("pred_strong_ctrl_dau",
                                     col("strong_ctrl_login_cnt") * col("strong_ctrl_login_cnt_per_dau"))
    base_data = base_data.withColumn("pred_normal_ctrl_dau",
                                     col("normal_ctrl_login_cnt") * col("normal_login_cnt_per_dau"))
    base_data = base_data.withColumn("pred_no_ctrl_dau", col("no_ctrl_login_cnt") * col("no_ctrl_login_cnt_per_dau"))

    base_data = base_data.withColumn("total_login_dau_ratio", lit(0.8))
    base_data = base_data.withColumn("pred_total_dau",
                                     col("pred_com_dau") + col("pred_free_dau") + col("pred_strong_ctrl_dau") + col(
                                         "pred_normal_ctrl_dau") + col("pred_no_ctrl_dau"))
    base_data = base_data.withColumn("pred_total_dau", col("pred_total_dau") * col("total_login_dau_ratio"))

    base_data = base_data.withColumn("end_time", lit(this_month_end))  # 本月最后1s
   
 #     base_data.write.format("orc").saveAsTable('dev.dev_yhzz_umc_algo_full0503', mode="overwrite")

    table_cols = ['user_log_acct', 'user_life_cycle_type_1st', 'model_a_1st', 'model_b_1st', 'goal_group_1st',
                  'annual_clv_1st', 'grid_name_1st', \
                  'model_a', 'model_b', 'model_c', 'model_l', 'user_life_cycle_type', \
                  'natural_pred_login_cnt_rest', 'com_login_cnt', 'free_login_cnt', 'strong_ctrl_login_cnt',
                  'normal_ctrl_login_cnt', 'no_ctrl_login_cnt', 'priority_level', \
                  'pred_com_dau', 'pred_free_dau', 'pred_strong_ctrl_dau', 'pred_normal_ctrl_dau', 'pred_no_ctrl_dau',
                  'total_login_dau_ratio', 'pred_total_dau', \
                  'login_cnt_mtd', 'natural_login_cnt_mtd', 'com_login_cnt_mtd', 'free_login_cnt_mtd',
                  'strong_ctrl_login_cnt_mtd', 'normal_ctrl_login_cnt_mtd', 'no_ctrl_login_cnt_mtd', \
                  'end_time', 'is_solvable', 'clv_pred_1m','priority_type' ]#
    print('table_cols', table_cols)
    
    result_data = base_data[table_cols].dropDuplicates(['user_log_acct'])
    # print("result cnt",result_data.count())

    result_data.createOrReplaceTempView('result_data_tmp')

    table_name = 'app.app_yhzz_umc_algo_pin_result'
    # table_name = 'app.app_yhzz_umc_algo_pin_result_new'

    # spark.sql("""DROP TABLE IF EXISTS dev.dev_test_0501""")  
    # print('dt=',day_ago_1)
    sql = """
       INSERT OVERWRITE TABLE {table_name} PARTITION (dt='{part_dt}' )
       --create table dev.dev_test_0501 as

        select
        *
        from
        result_data_tmp
    """.format(table_name=table_name, part_dt=day_ago_1)
    print(sql)

    
    # sql = """
    #    INSERT OVERWRITE TABLE {table_name} PARTITION (dt, user_priority_type, dp)

    #     select
    #     *,
    #     '{part_dt}' as dt,
    #     priority_type as user_priority_type,
    #     '{part_dp}' as dp
    #     from
    #     result_data_tmp
    # """.format(table_name=table_name, part_dt='{2022-03-31}', part_dp=dp)
    print(sql)
    
    
    spark.sql(sql)
    # current_time放在程序的末尾
    current_time = time.time()
    print("运行时间为" + str(current_time - old_time) + "s")
