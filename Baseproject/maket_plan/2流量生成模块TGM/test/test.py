import pandas as pd
import numpy as np
import math
import json

import sys
sys.path.append('..')
import src.solve_by_udf  as udf_func

# 获取基础数据

base_data = pd.read_csv('../data/base_data.csv',nrows=10)
base_data.head(2)

## 保量逻辑校验
pra1 = list(base_data.columns.values).index('com_login_cnt_last_month')
pra2 = list(base_data.columns.values).index('free_login_cnt_last_month')
pra3 = list(base_data.columns.values).index('strong_ctrl_login_cnt_last_month')
pra4 = list(base_data.columns.values).index('strong_ctrl_login_cnt_last_month')
pra5 = list(base_data.columns.values).index('no_ctrl_login_cnt_last_month')

basic_result_list = []
for indexs in base_data.index:
    print(base_data.loc[indexs].values[pra1:pra5])
    basic_result = udf_func.BasicGuarantee(base_data.loc[indexs].values[pra1],
                            base_data.loc[indexs].values[pra2],
                            base_data.loc[indexs].values[pra3],
                            base_data.loc[indexs].values[pra4],
                            base_data.loc[indexs].values[pra5])
    print(basic_result)
    basic_result_list.append(str(basic_result))

base_data['basic_result'] = basic_result_list

## 业务补量逻辑
par1 = list(base_data.columns.values).index('basic_result')
par2  = list(base_data.columns.values).index('priority_type')
year_clv_threshold = 2
par4  = list(base_data.columns.values).index('gmv_year')
par5 = list(base_data.columns.values).index('clv_pred_1m')
month_clv_threshold = 50
par7  = list(base_data.columns.values).index('com_login_cnt_last_month')
par8 = list(base_data.columns.values).index('free_login_cnt_last_month')
par9 = list(base_data.columns.values).index('strong_ctrl_login_cnt_last_month')
par10  = list(base_data.columns.values).index('normal_ctrl_login_cnt_last_month')
par11 = list(base_data.columns.values).index('no_ctrl_login_cnt_last_month')
par12 = list(base_data.columns.values).index('my_hash_code')
par13 = list(base_data.columns.values).index('month_hash')

businessrule_plan_list  = []
businessrule_no_allocation_channel_list = []
channel_businessrule_max_list =[]
channel_businessrule_max_add_list = []

for indexs in base_data.index:
    # print(base_data.loc[indexs].values[par1:par13])
    businessrule_plan, businessrule_no_allocation_channel, channel_businessrule_max, channel_businessrule_max_add = udf_func.BusinessRuleSupplement(base_data.loc[indexs].values[par1],
                                                                                                                                                    base_data.loc[indexs].values[par2],
                                                                                                                                                    year_clv_threshold,
                                                                                                                                                    base_data.loc[indexs].values[par4],
                                                                                                                                                    base_data.loc[indexs].values[par5],
                                                                                                                                                    month_clv_threshold,
                                                                                                                                                    base_data.loc[indexs].values[par7],
                                                                                                                                                    base_data.loc[indexs].values[par8],
                                                                                                                                                    base_data.loc[indexs].values[par9],
                                                                                                                                                    base_data.loc[indexs].values[par10],
                                                                                                                                                    base_data.loc[indexs].values[par11],
                                                                                                                                                    base_data.loc[indexs].values[par12],
                                                                                                                                                    base_data.loc[indexs].values[par13])
    businessrule_plan_list.append(businessrule_plan)
    businessrule_no_allocation_channel_list.append(businessrule_no_allocation_channel)
    channel_businessrule_max_list.append(channel_businessrule_max)
    channel_businessrule_max_add_list.append(channel_businessrule_max_add)

base_data['businessrule_plan'] = businessrule_plan_list
base_data['businessrule_no_allocation_channel'] = businessrule_no_allocation_channel_list
base_data['channel_businessrule_max'] = channel_businessrule_max_list
base_data['channel_businessrule_max_add'] = channel_businessrule_max_add_list


## 渠道上下限测试
columns_list = list(base_data.columns.values)
i_part_dt = columns_list.index('dt')
i_priority_type  = columns_list.index('priority_type')
i_my_hash_code = columns_list.index('my_hash_code')
i_channel_businessrule_max_add = columns_list.index('channel_businessrule_max_add')
i_channel_businessrule_max = columns_list.index('channel_businessrule_max')
i_com_max_login_count = columns_list.index('com_max_login_count')
i_free_max_login_count = columns_list.index('free_max_login_count')
i_strong_ctrl_max_login_count = columns_list.index('strong_ctrl_max_login_count')
i_normal_ctrl_max_login_count  = columns_list.index('normal_ctrl_max_login_count')
i_no_ctrl_max_login_count  = columns_list.index('no_ctrl_max_login_count')
i_com_login_cnt_last_month = columns_list.index('com_login_cnt_last_month')
i_free_login_cnt_last_month  = columns_list.index('free_login_cnt_last_month')
i_strong_ctrl_login_cnt_last_month = columns_list.index('strong_ctrl_login_cnt_last_month')
i_normal_ctrl_login_cnt_last_month = columns_list.index('normal_ctrl_login_cnt_last_month')
i_no_ctrl_login_cnt_last_month  = columns_list.index('no_ctrl_login_cnt_last_month')
i_is_acce_by_com = columns_list.index('is_acce_by_com')
i_is_acce_by_free = columns_list.index('is_acce_by_free')
i_is_acce_by_strong_ctrl = columns_list.index('is_acce_by_strong_ctrl')
i_is_acce_by_normal_ctrl = columns_list.index('is_acce_by_normal_ctrl') 
i_is_acce_by_no_ctrl = columns_list.index('is_acce_by_no_ctrl')
month_clv_threshold = 50
channel_max_adjust = 1.0
channel_preference = [1.0]
i_com_login_cnt_mtd = columns_list.index('com_login_cnt_mtd')
i_free_login_cnt_mtd = columns_list.index('free_login_cnt_mtd')
i_strong_ctrl_login_cnt_mtd = columns_list.index('strong_ctrl_login_cnt_mtd')
i_normal_ctrl_login_cnt_mtd = columns_list.index('normal_ctrl_login_cnt_mtd')
i_no_ctrl_login_cnt_mtd = columns_list.index('no_ctrl_login_cnt_mtd')
between_channel_adjust = 0

channel_max_login_days_list = []
for indexs in base_data.index:
    # print(base_data.loc[indexs].values[par1:par13])
    print("-"*10)
    channel_max_login_days = udf_func.get_user_channel_max_func(base_data.loc[indexs].values[i_part_dt],
                                                                    base_data.loc[indexs].values[i_priority_type],
                                                                    base_data.loc[indexs].values[i_my_hash_code],
                                                                    base_data.loc[indexs].values[i_channel_businessrule_max_add],
                                                                    base_data.loc[indexs].values[i_channel_businessrule_max],
                                                                    base_data.loc[indexs].values[i_com_max_login_count],
                                                                    base_data.loc[indexs].values[i_free_max_login_count],
                                                                    base_data.loc[indexs].values[i_strong_ctrl_max_login_count],
                                                                    base_data.loc[indexs].values[i_normal_ctrl_max_login_count],
                                                                    base_data.loc[indexs].values[i_no_ctrl_max_login_count],
                                                                    base_data.loc[indexs].values[i_com_login_cnt_last_month],
                                                                    base_data.loc[indexs].values[i_free_login_cnt_last_month],
                                                                    base_data.loc[indexs].values[i_strong_ctrl_login_cnt_last_month],
                                                                    base_data.loc[indexs].values[i_normal_ctrl_login_cnt_last_month],
                                                                    base_data.loc[indexs].values[i_no_ctrl_login_cnt_last_month],
                                                                    base_data.loc[indexs].values[i_is_acce_by_com],
                                                                    base_data.loc[indexs].values[i_is_acce_by_free],
                                                                    base_data.loc[indexs].values[i_is_acce_by_strong_ctrl],
                                                                    base_data.loc[indexs].values[i_is_acce_by_normal_ctrl],
                                                                    base_data.loc[indexs].values[i_is_acce_by_no_ctrl],
                                                                    month_clv_threshold,
                                                                    channel_max_adjust,
                                                                    channel_preference,
                                                                    base_data.loc[indexs].values[i_com_login_cnt_mtd],
                                                                    base_data.loc[indexs].values[i_free_login_cnt_mtd],
                                                                    base_data.loc[indexs].values[i_strong_ctrl_login_cnt_mtd],
                                                                    base_data.loc[indexs].values[i_normal_ctrl_login_cnt_mtd],
                                                                    base_data.loc[indexs].values[i_no_ctrl_login_cnt_mtd],
                                                                    between_channel_adjust)
    channel_max_login_days_list.append(channel_max_login_days)
    # print("-"*10)
base_data['channel_max_login_days'] = channel_max_login_days_list
