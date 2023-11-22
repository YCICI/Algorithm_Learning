# date: 2022-07-14
# author: yangyoupeng, yuchuchu

"""
用户管理中心-广义新用户-日规划
"""
import time
import datetime
import calendar
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import math
import sys
import os
import numpy as np

# old_time放在程序运行开始的地方
old_time = time.time()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, MapType
import pyspark.sql.functions as sf
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
spark.sql(
    """ADD JAR hdfs://ns1009/user/mart_jypt/mart_jypt_usr_grow/liuyang266/hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar""")
spark.sql("""CREATE TEMPORARY FUNCTION hash_sub AS 'com.jd.bdptools.ly.HashMonthly'""")

# applicationId:spark程序的唯一标识符，其格式取决于调度程序的实现
app_id = spark.sparkContext.applicationId
print(app_id)
print(spark.version)


# 渠道月规划
def get_monthplan_data(part_dt, priority_type):
    used_sql = """
        SELECT
            user_log_acct,
            user_life_cycle_type_1st,
            model_a_1st,
            model_b_1st,
            goal_group_1st,
            annual_clv_1st,
            grid_name_1st,
            clv_pred_1m,
            model_a,
            model_b,
            model_c,
            model_l,
            user_life_cycle_type,
            login_cnt_mtd,
            natural_login_cnt_mtd,
            com_login_cnt_mtd,
            free_login_cnt_mtd,
            strong_ctrl_login_cnt_mtd,
            normal_ctrl_login_cnt_mtd,
            no_ctrl_login_cnt_mtd,
            com_user_last_month_login_cnt,
            free_user_last_month_login_cnt,
            strong_ctrl_user_last_month_login_cnt,
            normal_ctrl_user_last_month_login_cnt,
            no_ctrl_user_last_month_login_cnt,
            com_user_base_guarteed_cnt,
            free_user_base_guarteed_cnt,
            strong_ctrl_user_base_guarteed_cnt,
            normal_ctrl_user_base_guarteed_cnt,
            no_ctrl_user_base_guarteed_cnt,
            com_user_acce_flag,
            free_user_acce_flag,
            strong_ctrl_user_acce_flag,
            normal_ctrl_user_acce_flag,
            no_ctrl_user_acce_flag,
            com_grid_max_login_cnt,
            free_grid_max_login_cnt,
            strong_ctrl_grid_max_login_cnt,
            normal_ctrl_grid_max_login_cnt,
            no_ctrl_grid_max_login_cnt,
            com_grid_last_month_total_cnt,
            free_grid_last_month_total_cnt,
            strong_ctrl_grid_last_month_total_cnt,
            normal_ctrl_grid_last_month_total_cnt,
            no_ctrl_grid_last_month_total_cnt,
            com_grid_quality_coeff,
            free_grid_quality_coeff,
            strong_ctrl_grid_quality_coeff,
            normal_ctrl_grid_quality_coeff,
            no_ctrl_grid_quality_coeff,
            com_grid_login_cnt_per_dau,
            free_grid_login_cnt_per_dau,
            strong_ctrl_grid_login_cnt_per_dau,
            normal_grid_login_cnt_per_dau,
            no_ctrl_grid_login_cnt_per_dau,
            com_grid_last_login_cost,
            free_grid_last_login_cost,
            strong_ctrl_grid_last_login_cost,
            normal_ctrl_grid_last_login_cost,
            no_ctrl_grid_last_login_cost,
            algo_total_info,
            NVL(natural_pred_login_cnt_rest, 0) natural_visit_cnt_month,
            NVL(com_login_cnt, 0) com_visit_cnt_month,
            NVL(free_login_cnt, 0) free_visit_cnt_month,
            NVL(strong_ctrl_login_cnt, 0) jd_strong_ctrl_visit_cnt_month,
            NVL(normal_ctrl_login_cnt, 0) jd_normal_ctrl_visit_cnt_month,
            NVL(no_ctrl_login_cnt, 0) jd_no_ctrl_visit_cnt_month,
            priority_level,
            pred_com_dau,
            pred_free_dau,
            pred_strong_ctrl_dau,
            pred_normal_ctrl_dau,
            pred_no_ctrl_dau,
            total_login_dau_ratio,
            pred_total_dau,
            is_solvable,
            end_time,
            is_malice_user,
            pred_natural_dau,
            natural_user_last_month_login_cnt,
            natural_grid_max_login_cnt,
            natural_grid_last_month_total_cnt,
            natural_grid_quality_coeff,
            natural_grid_login_cnt_per_dau,
            natural_last_login_cost,
            algo_com_login_cnt,
            algo_free_login_cnt,
            algo_strong_ctrl_login_cnt,
            algo_normal_ctrl_login_cnt,
            algo_no_ctrl_login_cnt,
            plan_flag,
            priority_type
        FROM
        	(
        		SELECT
        			*
        		FROM
        			app.app_yhzz_umc_algo_pin_interim
        		WHERE
        			dt = '{part_dt}'  --规划值
        			AND dp IN('low')
        			and priority_type in {priority_type_replace}
        	)
        	a
    """.format(part_dt=part_dt, priority_type_replace = priority_type)

    print("used sql", used_sql)
    data = spark.sql(used_sql)
    data.cache()
    print("data columns", data.columns)
    print("data dtypes", data.dtypes)

    return data


# 渠道同比分布
def get_channel_cum(start_date, end_date):
    # TODO：临时表改为app表
    used_sql = """
        SELECT
            dt,
            rate_natural_cnt,
            rate_com_cnt,
            rate_free_cnt,
            rate_strong_ctrl_cnt,
            rate_normal_ctrl_cnt,
            rate_no_ctrl_cnt
        FROM
            dev.dev_yhzz_day_plan_distribution
        WHERE
            dt >= '2021-07-01'
            AND dt <= '2021-07-31'
        group by
            dt,
            rate_natural_cnt,
            rate_com_cnt,
            rate_free_cnt,
            rate_strong_ctrl_cnt,
            rate_normal_ctrl_cnt,
            rate_no_ctrl_cnt
        ORDER BY
            dt ASC
    """.format(start_date=start_date, end_date=end_date)
    print("used sql", used_sql)
    data = spark.sql(used_sql)
    data.cache()
    print("data columns", data.columns)
    print("data dtypes", data.dtypes)

    return data


# 同比累计概率分布数组
def get_cum_probability(cum_probability_data, rest_day_cnt, day_end):
    """获取同比累计概率分布数组

    Args:
        cum_probability_data (_type_): _description_

    Returns:
        _type_: _description_
    """

    pd_data_channel_tb = cum_probability_data.toPandas()

    # 同比概率分布
    cum_probability = pd_data_channel_tb.drop(['dt'], axis=1).values

    # 累计概率分布
    cum_probability_2 = np.zeros((day_end, 6))
    for j in range(6):
        for i in range(day_end - rest_day_cnt, day_end):
            cum_probability_2[i, j] = np.sum(cum_probability[:i + 1, j])
    print('累计概率分布', cum_probability_2)

    return cum_probability


# 日规划：判断某个渠道是否大于'30'次,即对广义新人群主动渠道置2，高活置0
def tb_day_plan(channel_cnt_month, priority_type, channel_name):
    """
    """
    if channel_name == 'natural':
        channel_type = ['natural']
        channel_idx = 0
    elif channel_name == 'com':
        channel_type = ['com']
        channel_idx = 1
    elif channel_name == 'free':
        channel_type = ['free']
        channel_idx = 2
    elif channel_name == 'jd_strong_ctrl':
        channel_type = ['jd_strong_ctrl']
        channel_idx = 3
    elif channel_name == 'jd_normal_ctrl':
        channel_type = ['jd_normal_ctrl']
        channel_idx = 4
    else:
        channel_type = ['jd_no_ctrl']
        channel_idx = 5

    # 初始化
    day_cnt_dict = np.zeros(rest_day_cnt)


    # 渠道月规划
    channel_monthcnt = channel_cnt_month
    # 渠道同比概率
    channel_cum_probability = cum_probability[:, channel_idx]

    init_channel_daycnt = day_cnt_dict
    np.random.seed(channel_idx)

    if channel_monthcnt > rest_day_cnt:
        a = 1
        channel_dayplan_list_1 = np.array([a for i in range(rest_day_cnt)])  # 先每天置1保底
        flag = 0
        channel_dayplan_list = channel_dayplan_list_1 + monthcnt_to_daycnt(init_channel_daycnt,
                                                                           channel_monthcnt-a*rest_day_cnt,
                                                                           channel_cum_probability,
                                                                           rest_day_cnt,
                                                                           flag)  # 剩余天数按同比分布，且不微调
    else:
        flag = 1
        channel_dayplan_list = monthcnt_to_daycnt(init_channel_daycnt,
                                                  channel_monthcnt,
                                                  channel_cum_probability,
                                                  rest_day_cnt,
                                                  flag)
    # print(channel_dayplan_list)

    # 若计划执行首日为0则置为2
    if (priority_type == 'gy_new_1' or priority_type == 'gy_new_2') and (channel_name == 'com' or channel_name == 'jd_normal_ctrl'):
        if channel_dayplan_list[0] < 1:
            channel_dayplan_list[0] = 2
    day_cnt_dict = ','.join(map(str, channel_dayplan_list))
    return day_cnt_dict


# 日规划核心拆解逻辑：
def monthcnt_to_daycnt(init_channel_daycnt, channel_monthcnt, channel_cum_probability, rest_day_cnt, flag):
    """_summary_

    Args:
        init_channel_daycnt (_type_): _description_
        channel_monthcnt (_type_): _description_
        channel_cum_probability (_type_): _description_

    Returns:
        _type_: _description_
    """
    channel_daycnt = init_channel_daycnt

    if channel_monthcnt > 0:
        day_select = np.zeros((1, channel_monthcnt))
        # 生成natural_cnt个随机数 用来判断拆到哪些天
        rand_seed = np.random.rand(1, channel_monthcnt)
        for i in range(channel_monthcnt):
            # 将随机数和累积分布挨个对比，概率大的那天区间也大，被选中的概率自然大
            for j in channel_cum_probability:
                if j >= rand_seed[0, i]:
                    day_select[0, i] = channel_cum_probability.tolist().index(j) + 1
                    break
        # print("day_select:", day_select)
        # 微调，作用是若有的天被分配大于1次，则将这天重新打散分配，用create_uniques函数
        if flag == 1:
            day_select = create_uniques(np.array(day_select[0]).astype('int64'), rest_day_cnt)
            day_idx = np.stack(day_select.astype('int64'))
            channel_daycnt[day_idx - 1] = 1
        else:
            day_idx = np.stack(day_select.astype('int64'))
            for k in day_idx[0,:]:
                channel_daycnt[k - 1] += 1
    else:
        pass

    return channel_daycnt


def create_uniques(arr, rest_day_cnt):
    """_summary_

    Args:
        arr (_type_): _description_

    Returns:
        _type_: _description_
    """
    unq, c = np.unique(arr, return_counts=1)
    m = np.isin(arr, unq[c > 1])

    newvals = np.setdiff1d(np.arange(rest_day_cnt), arr[~m])
    np.random.shuffle(newvals)

    cnt = m.tolist().count(True)
    newvals = newvals[:cnt]
    arr[m] = newvals

    return arr


def parse_res(data):
    """解析json字符串
    Args:
        data (_type_): _description_

    Returns:
        _type_: _description_
    """

    return data


@udf
def get_first(x):
    return int(float(x.split(',')[0]))


def data_insert_interim_table(data, table_name, table_dt, tabel_dp):
    # result_data = data[table_cols]

    result_data = data.withColumn('natural_today', get_first(sf.col('natural'))).\
        withColumn('com_today', get_first(sf.col('com'))).\
        withColumn('free_today', get_first(sf.col('free'))). \
        withColumn('jd_strong_today', get_first(sf.col('jd_strong_ctrl'))).\
        withColumn('jd_normal_today', get_first(sf.col('jd_normal_ctrl'))).\
        withColumn('jd_no_ctrl_today', get_first(sf.col('jd_no_ctrl')))
    #     data.show(20)

    result_data.createOrReplaceTempView('result_data_tmp')

    sql = """
       INSERT OVERWRITE TABLE {table_name} PARTITION (dt='{part_dt}', dp, priority_type)

        select
            user_log_acct,
            user_life_cycle_type_1st,
            model_a_1st,
            model_b_1st,
            goal_group_1st,
            annual_clv_1st,
            grid_name_1st,
            clv_pred_1m,
            model_a,
            model_b,
            model_c,
            model_l,
            user_life_cycle_type,
            login_cnt_mtd,
            natural_login_cnt_mtd,
            com_login_cnt_mtd,
            free_login_cnt_mtd,
            strong_ctrl_login_cnt_mtd,
            normal_ctrl_login_cnt_mtd,
            no_ctrl_login_cnt_mtd,
            com_user_last_month_login_cnt,
            free_user_last_month_login_cnt,
            strong_ctrl_user_last_month_login_cnt,
            normal_ctrl_user_last_month_login_cnt,
            no_ctrl_user_last_month_login_cnt,
            com_user_base_guarteed_cnt,
            free_user_base_guarteed_cnt,
            strong_ctrl_user_base_guarteed_cnt,
            normal_ctrl_user_base_guarteed_cnt,
            no_ctrl_user_base_guarteed_cnt,
            com_user_acce_flag,
            free_user_acce_flag,
            strong_ctrl_user_acce_flag,
            normal_ctrl_user_acce_flag,
            no_ctrl_user_acce_flag,
            com_grid_max_login_cnt,
            free_grid_max_login_cnt,
            strong_ctrl_grid_max_login_cnt,
            normal_ctrl_grid_max_login_cnt,
            no_ctrl_grid_max_login_cnt,
            com_grid_last_month_total_cnt,
            free_grid_last_month_total_cnt,
            strong_ctrl_grid_last_month_total_cnt,
            normal_ctrl_grid_last_month_total_cnt,
            no_ctrl_grid_last_month_total_cnt,
            com_grid_quality_coeff,
            free_grid_quality_coeff,
            strong_ctrl_grid_quality_coeff,
            normal_ctrl_grid_quality_coeff,
            no_ctrl_grid_quality_coeff,
            com_grid_login_cnt_per_dau,
            free_grid_login_cnt_per_dau,
            strong_ctrl_grid_login_cnt_per_dau,
            normal_grid_login_cnt_per_dau,
            no_ctrl_grid_login_cnt_per_dau,
            com_grid_last_login_cost,
            free_grid_last_login_cost,
            strong_ctrl_grid_last_login_cost,
            normal_ctrl_grid_last_login_cost,
            no_ctrl_grid_last_login_cost,
            algo_total_info,
            natural_visit_cnt_month,
            com_visit_cnt_month,
            free_visit_cnt_month,
            jd_strong_ctrl_visit_cnt_month,
            jd_normal_ctrl_visit_cnt_month,
            jd_no_ctrl_visit_cnt_month,
            priority_level,
            pred_com_dau,
            pred_free_dau,
            pred_strong_ctrl_dau,
            pred_normal_ctrl_dau,
            pred_no_ctrl_dau,
            total_login_dau_ratio,
            pred_total_dau,
            is_solvable,
            end_time,
            is_malice_user,
            pred_natural_dau,
            natural_user_last_month_login_cnt,
            natural_grid_max_login_cnt,
            natural_grid_last_month_total_cnt,
            natural_grid_quality_coeff,
            natural_grid_login_cnt_per_dau,
            natural_last_login_cost,
            algo_com_login_cnt,
            algo_free_login_cnt,
            algo_strong_ctrl_login_cnt,
            algo_normal_ctrl_login_cnt,
            algo_no_ctrl_login_cnt,
            plan_flag,
            '' day_cnt,
            natural_today,
            com_today,
            free_today,
            jd_strong_today,
            jd_normal_today,
            jd_no_ctrl_today,
            natural AS natural_day_rest,
            com AS com_day_rest,
            free AS free_day_rest,
            jd_strong_ctrl AS jd_strong_day_rest,
            jd_normal_ctrl AS jd_normal_day_res,
            jd_no_ctrl AS jd_no_ctrl_day_res,
            '{part_dp}' dp,
            priority_type
        from
            result_data_tmp     

    """.format(table_name=table_name, part_dt=table_dt, part_dp=tabel_dp)
    print(sql)
    spark.sql(sql)
    print("successful data insert tabel")

    return


def data_insert_result_table(table_dt, tabel_dp):
    sql = """
        INSERT OVERWRITE TABLE app.app_yhzz_umc_algo_pin_result_day_plan PARTITION (dt)

        SELECT
            a.user_log_acct,
            user_life_cycle_type_1st,
            model_a_1st ,
            model_b_1st ,
            goal_group_1st ,
            clv_pred_1m clv_level_1st ,
            grid_name_1st,

            model_a,
            model_b,
            model_c,
            model_l,
            user_life_cycle_type,

            natural_visit_cnt_month,
            com_visit_cnt_month,   
            free_visit_cnt_month ,
            jd_strong_ctrl_visit_cnt_month,
            jd_normal_ctrl_visit_cnt_month,
            jd_no_ctrl_visit_cnt_month,    
            priority_level ,

            pred_com_dau   ,
            pred_free_dau  ,
            pred_strong_ctrl_dau,
            pred_normal_ctrl_dau,
            pred_no_ctrl_dau    ,
            total_login_dau_ratio ,
            pred_total_dau ,

            login_cnt_mtd ,
            natural_login_cnt_mtd  ,
            com_login_cnt_mtd   ,
            free_login_cnt_mtd  ,
            strong_ctrl_login_cnt_mtd ,
            normal_ctrl_login_cnt_mtd ,
            no_ctrl_login_cnt_mtd   ,
            end_time ,

            is_solvable   ,
            clv_pred_1m   ,
            priority_type ,

            day_cnt   ,
            natural_today  ,
            com_today ,
            free_today,
            jd_strong_today  ,
            jd_normal_today  ,
            jd_no_ctrl_today,

            natural_day_rest ,
            com_day_rest  ,
            free_day_rest ,
            jd_strong_day_rest  ,
            jd_normal_day_res   ,
            jd_no_ctrl_day_res,

            CASE WHEN priority_type in ('gy_new_1', 'gy_new_2')
                THEN distribution_channel
                ELSE 2
                END distribution_channel,
            CASE WHEN priority_type in ('gy_new_1', 'gy_new_2')
                THEN markup_factor
                ELSE 1
                END markup_factor,    
            CASE WHEN priority_type in ('gy_new_1', 'gy_new_2')
                THEN fatigue_threshold
                ELSE 50
                END fatigue_threshold,

            dt
        FROM
            (
                SELECT
                    *
                FROM
                    app.app_yhzz_umc_algo_pin_interim_day_plan
                WHERE
                    dt='{part_dt}'
                    and dp='{part_dp}'
            )
            a
        LEFT JOIN
            (
                SELECT
                    user_log_acct,
                    distribution_channel,
                    markup_factor,
                    fatigue_threshold
                FROM
                    app.app_yhzz_umc_cda_result
            )
            b
        ON
            a.user_log_acct = b.user_log_acct
        WHERE   
            dt='{part_dt}'
            and dp='{part_dp}'

    """.format(part_dt=table_dt, part_dp=tabel_dp)
    print(sql)
    spark.sql(sql)
    print("successful data conver tabel")

    return


def get_current_month_rest_day(sdate):
    # 字符串转时间date
    datetime_now = datetime.datetime.strptime(sdate, '%Y-%m-%d').date()
    end = calendar.monthrange(datetime_now.year, datetime_now.month)[1]
    end_date = '%s-%s-%s' % (datetime_now.year, datetime_now.month, end)
    day_end = int(str(end_date)[7:])
    day_now = int(str(datetime_now)[8:])
    rest_day = day_end - day_now + 1

    return rest_day, day_end


def get_last_year_date(sdate):
    datetime_now = datetime.datetime.strptime(sdate, '%Y-%m-%d').date()
    last_year_date = datetime_now + relativedelta(months=-12)
    end = calendar.monthrange(last_year_date.year, last_year_date.month)[1]
    start_date = '%s-%s-01' % (last_year_date.year, last_year_date.month)
    end_date = '%s-%s-%s' % (last_year_date.year, last_year_date.month, end)
    day_start = str(datetime.datetime.strptime(start_date, '%Y-%m-%d'))[:10]
    day_end = str(datetime.datetime.strptime(end_date, '%Y-%m-%d'))[:10]

    return day_start, day_end


if __name__ == "__main__":
    # 传入参数
    day_ago_1 = sys.argv[1]  # T-1
    priority_type = sys.argv[2]
    table_name = 'app.app_yhzz_umc_algo_pin_interim_day_plan'
    dp = 'low'
    # 与天数相关的参数
    start_date, end_date = get_last_year_date(day_ago_1)
    rest_day_cnt, day_end = get_current_month_rest_day(day_ago_1)

    print('-----model params-----')
    print('day_ago_1(table_dt):   ', day_ago_1)
    print('table_name:            ', table_name)
    print('rest_day_cnt（当月剩余天数）:   ', rest_day_cnt)
    print('day_end（当月最后一天）:            ', day_end)
    print('start_date（同比开始日期）:   ', start_date)
    print('end_date（同比结束日期）:            ', end_date)

    print(">" * 10, "get base data")

    # 获取月规划
    month_cnt_data = get_monthplan_data(day_ago_1, priority_type)

    # 获取渠道同比分布
    cum_probability_data = get_channel_cum(start_date, end_date)
    # 获取渠道同比累计概率分布
    print(">" * 10, "获取渠道同比累计概率分布")
    cum_probability = get_cum_probability(cum_probability_data, rest_day_cnt, day_end)
    print('初始累积概率分布：', cum_probability)
    # 取剩余天数的累积概率
    cum_probability = cum_probability[day_end - rest_day_cnt - 1:, :]
    print('取剩余天数的累积概率分布：', cum_probability)
    # 归一化
    for i in range(cum_probability.shape[1]):
        max_value = cum_probability[:, i].max()
        min_value = cum_probability[:, i].min()
        cum_probability[:, i] = (cum_probability[:, i] - min_value) / (max_value - min_value)
    # 去掉首行
    cum_probability = cum_probability[1:, :]
    print('归一化后的累积概率分布：', cum_probability)

    # 生成日规划
    print(">" * 10, "生成日规划")


    month_cnt_data = month_cnt_data.withColumn('natural', udf(lambda x,y: tb_day_plan(x,y,'natural'))('natural_visit_cnt_month',
                                                                      'priority_type')).\
                                    withColumn('com', udf(lambda x,y: tb_day_plan(x,y,'com'))('com_visit_cnt_month',
                                                                      'priority_type')). \
                                    withColumn('free', udf(lambda x, y: tb_day_plan(x, y, 'free'))('free_visit_cnt_month',
                                                                     'priority_type')). \
                                    withColumn('jd_strong_ctrl', udf(lambda x, y: tb_day_plan(x, y, 'jd_strong_ctrl'))('jd_strong_ctrl_visit_cnt_month',
                                                                       'priority_type')). \
                                    withColumn('jd_normal_ctrl', udf(lambda x, y: tb_day_plan(x, y, 'jd_normal_ctrl'))('jd_normal_ctrl_visit_cnt_month',
                                                                         'priority_type')). \
                                    withColumn('jd_no_ctrl',  udf(lambda x, y: tb_day_plan(x, y, 'jd_no_ctrl'))('jd_no_ctrl_visit_cnt_month',
                                                                         'priority_type'))

    # 处理数据 写中间表
    print(">" * 10, "处理数据 写中间表")
    # month_cnt_data = parse_res(month_cnt_data)

    data_insert_interim_table(data=month_cnt_data,
                              table_name=table_name,
                              table_dt=day_ago_1,
                              tabel_dp=dp)

    # # 处理数据 写结果表
    # print(">" * 10, "处理数据 写结果表")
    # # month_cnt_data = parse_res(month_cnt_data)
    #
    # data_insert_result_table(table_dt=day_ago_1,
    #                          tabel_dp=dp)

    # current_time放在程序的末尾
    current_time = time.time()
    print("运行时间为" + str(current_time - old_time) + "s")




