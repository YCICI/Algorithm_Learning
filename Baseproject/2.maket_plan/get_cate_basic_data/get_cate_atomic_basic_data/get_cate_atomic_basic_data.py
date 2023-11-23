#!/usr/bin/env python3
# coding: utf-8

import pandas as pd
import os
import sys
import datetime
import numpy as np
import imp


from pyspark.sql import SparkSession


imp.reload(sys)
# if sys.getdefaultencoding() != 'utf-8':
# 	reload(sys)
#sys.setdefaultencoding('utf-8')

# os.environ['PYSPARK_PYTHON'] = '/usr/local/anaconda3/bin/python3.6'
spark = (SparkSession
    .builder
    .appName("test-dockerlinuxcontainer")
    .enableHiveSupport()
    .getOrCreate())
# applicationId:spark程序的唯一标识符，其格式取决于调度程序的实现
app_id = spark.sparkContext.applicationId 
print(app_id)
print(spark.version)



# 基础信息定义
cate_list = ('1320','15901','1620')
sta_ids=['C10001','C10002','C10003']
sta_info = {#'C10000': ['离站补水', '8', 'app_push', '实时活动'],
			'C10001': ['主动触达', '8', 'app_push', '离线活动'],
			'C10002': ['离站拦截', '12', '离站拦截', '实时活动'],
			'C10003': ['站内信', '1', '站内信', '实时活动']}

people_id=['C0000', 'C0001', 'C0002', 'C0003', 'C0004', 'C0005', 'C0006', 'C0007', 'C0008', 'C0009', 'C0010', 'C0011', 'C0012', 'C0013', 'C0014', 'C0015']

people_info = {'C0000': ['品类人群', 'cate_none'],
				'C0001': ['品类_购物车', 'cate_cart'],
				'C0002': ['品类_登录', 'cate_login'],
				'C0003': ['品类_登录_购物车', 'cate_login_cart'],
				'C0004': ['品类_高潜', 'cate_faster'],
				'C0005': ['品类_高潜_购物车', 'cate_faster_cart'],
				'C0006': ['品类_高潜_登陆', 'cate_faster_login'],
				'C0007': ['品类_高潜_登陆_购物车', 'cate_faster_login_cart'],
				'C0008': ['品类_push', 'cate_push'],
				'C0009': ['品类_push_购物车', 'cate_push_cart'],
				'C0010': ['品类_push_登陆', 'cate_push_login'],
				'C0011': ['品类_push_登陆_购物车', 'cate_push_login_cart'],
				'C0012': ['品类_push_高潜', 'cate_push_faster'],
				'C0013': ['品类_push_高潜_购物车', 'cate_push_faster_cart'],
				'C0014': ['品类_push_高潜_登陆', 'cate_push_faster_login'],
			    'C0015': ['品类_push_高潜_登陆_购物车', 'cate_push_faster_login_cart']}


def init_atomic_people(sta_ids,sta_info,people_id,people_info):
	# 初始化原子人群信息
	init_df = pd.DataFrame({'prospect_id':[],
							'prospect_channel_id':[],
							'prospect_channel_name':[],
							'prospect_channel_type':[],
							'prospect_description':[],
							'prospect_script':[],
							'scene_id':[],
							'prospect_scene_name':[]}) 
	prospect_id_list=[]
	channel_id_list=[]
	channel_name_list=[]
	channel_type_list =[]
	description_list = []
	scrip_list = []
	scene_id_list=[]
	scene_name_list = []


	for sta in sta_ids:
		scene_id = sta
		
		scene_name = sta_info[sta][0]
		sta_channel_id = sta_info[sta][1]
		sta_channel_name = sta_info[sta][2]
		sta_type = sta_info[sta][3]
		
		if sta_type=='实时活动':
			
			scene_id_list.append(scene_id)
			prospect_id_list.append('C100')
			channel_id_list.append(sta_channel_id)
			channel_name_list.append(sta_channel_name)
			channel_type_list.append(sta_type)
			description_list.append('品类_实时')
			scrip_list.append('cate_online') 
			scene_name_list.append(scene_name)

		if sta_type=='离线活动':
			for p in people_id:
				id = p
				description = people_info[p]
				scene_id_list.append(scene_id)
				prospect_id_list.append(id)
				channel_id_list.append(sta_channel_id)
				channel_name_list.append(sta_channel_name)
				channel_type_list.append(sta_type)
				description_list.append(description[0])
				scrip_list.append(description[1]) 
				scene_name_list.append(scene_name)
		
			
	init_df['prospect_id']=prospect_id_list  
	init_df['prospect_channel_id']=channel_id_list			
	init_df['prospect_channel_name']=channel_name_list
	init_df['prospect_channel_type']=channel_type_list
	init_df['prospect_description']=description_list
	init_df['prospect_script']=scrip_list
	init_df['scene_id']=scene_id_list
	init_df['prospect_scene_name']=scene_name_list
	init_df['prospect_channel_lvl']=0
	init_df['prospect_name']=init_df['prospect_scene_name']+'_'+ init_df['prospect_description']
	
	return init_df

def count_atomic_number(part_dt,cate_list):
	# 品类原子人群计数
	used_sql= """
		SELECT
			dt,
			item_first_cate_cd,
			user_type,
			CASE
				WHEN cate_none = 1
				THEN 'cate_none'
				WHEN cate_cart = 1
				THEN 'cate_cart'
				WHEN cate_login = 1
				THEN 'cate_login'
				WHEN cate_login_cart = 1
				THEN 'cate_login_cart'
				WHEN cate_faster = 1
				THEN 'cate_faster'
				WHEN cate_faster_cart = 1
				THEN 'cate_faster_cart'
				WHEN cate_faster_login = 1
				THEN 'cate_faster_login'
				WHEN cate_faster_login_cart = 1
				THEN 'cate_faster_login_cart'
				WHEN cate_push = 1
				THEN 'cate_push'
				WHEN cate_push_cart = 1
				THEN 'cate_push_cart'
				WHEN cate_push_login = 1
				THEN 'cate_push_login'
				WHEN cate_push_login_cart = 1
				THEN 'cate_push_login_cart'
				WHEN cate_push_faster = 1
				THEN 'cate_push_faster'
				WHEN cate_push_faster_cart = 1
				THEN 'cate_push_faster_cart'
				WHEN cate_push_faster_cart = 1
				THEN 'cate_push_faster_cart'
				WHEN cate_push_faster_login_cart = 1
				THEN 'cate_push_faster_login_cart'
				else 'cate_none'
			END AS prospect_script,
			COUNT(DISTINCT user_log_acct) as prospect_channel_cnt
		FROM
		app.app_yhzz_juge_catenew_atomic_prospect_label_d
		WHERE
		dt='{part_dt}'
		AND item_first_cate_cd in {cate_list}
		GROUP BY
		CASE
				WHEN cate_none = 1
				THEN 'cate_none'
				WHEN cate_cart = 1
				THEN 'cate_cart'
				WHEN cate_login = 1
				THEN 'cate_login'
				WHEN cate_login_cart = 1
				THEN 'cate_login_cart'
				WHEN cate_faster = 1
				THEN 'cate_faster'
				WHEN cate_faster_cart = 1
				THEN 'cate_faster_cart'
				WHEN cate_faster_login = 1
				THEN 'cate_faster_login'
				WHEN cate_faster_login_cart = 1
				THEN 'cate_faster_login_cart'
				WHEN cate_push = 1
				THEN 'cate_push'
				WHEN cate_push_cart = 1
				THEN 'cate_push_cart'
				WHEN cate_push_login = 1
				THEN 'cate_push_login'
				WHEN cate_push_login_cart = 1
				THEN 'cate_push_login_cart'
				WHEN cate_push_faster = 1
				THEN 'cate_push_faster'
				WHEN cate_push_faster_cart = 1
				THEN 'cate_push_faster_cart'
				WHEN cate_push_faster_cart = 1
				THEN 'cate_push_faster_cart'
				WHEN cate_push_faster_login_cart = 1
				THEN 'cate_push_faster_login_cart'
				else 'cate_none'
			END,
			dt,
			item_first_cate_cd,
			user_type
			order by
			user_type,
			item_first_cate_cd
			
	""".format(part_dt = part_dt,cate_list=cate_list)
	print(used_sql)
	data = spark.sql(used_sql)
	df= data.toPandas()
	return df


def get_channel_rate_data(part_dt,cate_list):
	used_sql =  """
	SELECT
			item_first_cate_cd,
			user_type,
			prospect_channel_id,
			prospect_channel_name,
			prospect_channel_type,
			prospect_scene_name,
			prospect_script,
			juge_inter_cnt ,
			inter_success_cnt as juge_inter_success_cnt,
			expouse_cnt,
			buy_cnt,
			cps_cost 
	 FROM 
		  app.app_yhzz_cate_juge_channel_inf_d
	 where dt = '{part_dt}'
		   and item_first_cate_cd in {cate_list}
		   AND prospect_channel_id is not null
	""".format(part_dt = part_dt,cate_list=cate_list)

	print(used_sql)
	data = spark.sql(used_sql)
	c_df = data.toPandas()
	return c_df

def get_online_level_data(part_dt,cate_list):
	used_sql = """
			SELECT
				dt,
				cate_id as item_first_cate_cd,
				channel_id as prospect_channel_id,
				case when user_type='品类老' then '6'
					when user_type='品类新' then '1'
					end as user_type,
				task_level as juge_task_level,
				group_level as juge_group_level,
				CASE WHEN (channel_id='1') THEN '站内信'
					WHEN (channel_id='12') THEN '离站拦截'
					END AS prospect_scene_name,
				'cate_online' as prospect_script,
				ka_gy_user as juge_inter_cnt, --渠道干预规模
				gy_user as prospect_channel_cnt,  --渠道规模
				ka_gy_user/gy_user  as juge_channel_inter_rate --渠道干预率

			FROM 
				app.app_yhzz_cate_expo_rate_simulation
			where 
				dt='2022-04-17' 
				and cate_id in {cate_id_list}
				and task_level = 2
			""".format(part_dt = part_dt,cate_id_list = cate_list)
	print(used_sql)
	online_data = spark.sql(used_sql)
	level_df = online_data.toPandas()
	return level_df
			

def channel_rate_cal(df):
	channel_cnt_list = df['prospect_channel_cnt'].values[:]
	juge_inter_list= df['juge_inter_cnt'].values[:]
	juge_inter_success_list = df['juge_inter_success_cnt'].values[:]  ## 要改为inter_success_cnt
	buy_cnt_list = df['buy_cnt'].values[:]
	cps_cost_list = df['cps_cost'].values[:]
	
	#
	df['juge_channel_inter_rate'] =  juge_inter_list/channel_cnt_list 
	df['juge_channel_inter_success_rate'] = juge_inter_success_list/juge_inter_list
	df['prospect_channel_cvr'] = buy_cnt_list/juge_inter_success_list
	df['prospect_channel_cac'] = cps_cost_list/buy_cnt_list

	#
	df['prospect_channel_incr_cvr'] = 0.2
	df['prospect_channel_incr_p10k'] = 0.2 #0.1*np.random.randint(0,100,size=(len(t_df),1))
	df['prospect_channel_incr_pct'] = 0.05
	df['prospect_channel_incr_cac'] = 5

	#
	df['juge_inter_attenuation_para']=0.8
	df['juge_cate_conflict_para']=0.8

	return df


def check_data(df):
    flag = False
    if len(df)==0:
        print("have no data! please check!")
        flag = True
    return flag


def data_ala_concat(count_df,init_df,channel_df,level_df):

	print('0 channel_df',len(channel_df))
	print('channel_df',channel_df)
	
	print(channel_df.info())
	
	#print(channel_df[channel_df['prospect_channel_type']=='离线活动'])
	outline_df = init_df[init_df['prospect_channel_type']=='离线活动']
	online_df  = init_df[init_df['prospect_channel_type']=='实时活动']
	#print(outline_df.info())

	print('1 the number of outline_df:',len(outline_df))
	print('2 the number of online_df:',len(online_df))

	#离线原子人群合并
	out_channel_df= channel_df[channel_df['prospect_channel_type']=='离线活动']
	outline_keys =['item_first_cate_cd','user_type','prospect_channel_id','prospect_channel_name','prospect_script']
	## 有问题的可能是这个字段 prospect_channel_type
	#outline_keys = ['item_first_cate_cd','user_type','prospect_channel_id','prospect_script']
	count_init_outline_df = pd.merge(outline_df,count_df,how='left',on=['prospect_script'])
	init_outline_df = pd.merge(count_init_outline_df,out_channel_df,how='left',on=outline_keys)

	print(init_outline_df.columns)


	# 问题定位，中文编码强制转为utf8之后无法判断相等
	# test6 = pd.merge(count_init_outline_df,out_channel_df,how='left',on=['item_first_cate_cd','user_type','prospect_channel_id','prospect_channel_name'])

	# print('it is test')
	# print('test6',test6)
	
	print('3 the number of count_init_outline_df:',len(count_init_outline_df))
	print('count_init_outline_df',count_init_outline_df)
	print('init_outline_df',init_outline_df)
	print('init_outline_df juge_inter_cnt',dict(init_outline_df['juge_inter_cnt'].value_counts()))


	init_outline_df = init_outline_df[init_outline_df['juge_inter_success_cnt']>1000]
	print('init_outline_df juge_inter_cnt>10000',init_outline_df['juge_inter_cnt'].value_counts())


	init_outline_df['juge_task_level']=2
	init_outline_df['juge_group_level']=3
	print('4 the number of init_outline_df:',len(init_outline_df))
	print('init_outline_df',init_outline_df)
	init_outline_df = init_outline_df[['prospect_id', 'prospect_channel_id', 'prospect_channel_name',
       'prospect_channel_type_x', 'prospect_description', 'prospect_script',
       'scene_id', 'prospect_scene_name_x', 'prospect_channel_lvl',
       'prospect_name', 'dt', 'item_first_cate_cd', 'user_type',
       'prospect_channel_cnt', 'juge_inter_cnt', 'juge_inter_success_cnt',
       'expouse_cnt', 'buy_cnt', 'cps_cost']]
	init_outline_df.columns = ['prospect_id', 'prospect_channel_id', 'prospect_channel_name',
		'prospect_channel_type', 'prospect_description', 'prospect_script',
		'scene_id', 'prospect_scene_name', 'prospect_channel_lvl',
		'prospect_name', 'dt', 'item_first_cate_cd', 'user_type',
		'prospect_channel_cnt', 'juge_inter_cnt', 'juge_inter_success_cnt',
		'expouse_cnt', 'buy_cnt', 'cps_cost']
	print(init_outline_df.columns)



	# 实时原子人群
	online_keys = ['item_first_cate_cd','user_type','prospect_channel_type','prospect_channel_id']

	count_init_online_df = pd.merge(online_df,level_df,how='left',on=['prospect_channel_id','prospect_script'])
	print('5 the number of count_init_online_df:',len(count_init_online_df))
	online_channle_df = channel_df[channel_df.prospect_channel_type=='实时活动'].groupby(online_keys)\
																.agg({#'juge_inter_cnt':'sum',
																	'juge_inter_success_cnt':'sum',
																	'expouse_cnt':'sum',
																	'buy_cnt':'sum',
																	'cps_cost':'sum'}).reset_index()
	print(' 6 the number of online_channle_df:',len(online_channle_df))
	print('online_channle_df',online_channle_df)
	init_online_df = pd.merge(count_init_online_df,online_channle_df,how='left',on=online_keys)
	print('7 the number of init_online_df:',len(init_online_df))
	init_online_df.prospect_id = init_online_df.prospect_id + init_online_df.juge_task_level + init_online_df.juge_group_level
	init_online_df = init_online_df[['prospect_id', 'prospect_channel_id', 'prospect_channel_name',
       'prospect_channel_type', 'prospect_description', 'prospect_script',
       'scene_id', 'prospect_scene_name_x', 'prospect_channel_lvl',
       'prospect_name', 'dt', 'item_first_cate_cd', 'user_type',
       'juge_task_level', 'juge_group_level', 
       'juge_inter_cnt', 'prospect_channel_cnt', 'juge_channel_inter_rate',
       'juge_inter_success_cnt', 'expouse_cnt', 'buy_cnt', 'cps_cost']]
	   
	init_online_df.columns = ['prospect_id', 'prospect_channel_id', 'prospect_channel_name',
		'prospect_channel_type', 'prospect_description', 'prospect_script',
		'scene_id', 'prospect_scene_name', 'prospect_channel_lvl',
		'prospect_name', 'dt', 'item_first_cate_cd', 'user_type',
		'juge_task_level', 'juge_group_level', 
		'juge_inter_cnt', 'prospect_channel_cnt', 'juge_channel_inter_rate',
		'juge_inter_success_cnt', 'expouse_cnt', 'buy_cnt', 'cps_cost']

	print("="*10,"check outline_df & online_df")
	check_list = [check_data(init_online_df),check_data(init_outline_df)]
	if any(check_list):
		print("[init_online_df,init_outline_df] is null?",check_list)
		sys.exit()
	else:
		print("outline_df & online_df is good")

	##  实时离线合并
	result_df =  pd.concat([init_online_df,init_outline_df])
	print('the number of result_df:',len(result_df))

	##  处理任务参数
	result_df= result_df[result_df.user_type.notna()]
	result_df= result_df[result_df.prospect_channel_name.notna()]
	result_df = channel_rate_cal(result_df)


	return result_df

# today = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d')
# day_ago_2 = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=2),'%Y-%m-%d')
# day_ago_3 = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=4),'%Y-%m-%d')
# day_ago_4 = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=5),'%Y-%m-%d')

if __name__ == '__main__':

	args = sys.argv
	print(args)
	# table_dt = day_ago_3
	# part_dt = day_ago_4
	part_dt = sys.argv[1] #一般传t-1的参数进来
	cate_list = sys.argv[2]

	print("part_dt",part_dt)

	#获取基础数据
	init_df = init_atomic_people(sta_ids,sta_info,people_id,people_info)
	count_df = count_atomic_number(part_dt=part_dt,cate_list=cate_list)
	
	channel_df = get_channel_rate_data(part_dt=part_dt,cate_list=cate_list)
	level_df = get_online_level_data(part_dt=part_dt,cate_list=cate_list)

	# 检测空数据，返回
	print("="*10,"check data")
	check_list = [check_data(count_df),check_data(init_df),check_data(channel_df),check_data(level_df)]
	if any(check_list):
		print("[count_df,init_df,channel_df,level_df] is null?",check_list)
		sys.exit()
	else:
		print("data is good")


	result_df = data_ala_concat(count_df,init_df,channel_df,level_df)

    #  数据基础处理

	table_cols = ['prospect_id', 'prospect_channel_id', 'prospect_channel_name','prospect_channel_type', 'prospect_description', 'prospect_script','scene_id', 'prospect_scene_name', 'prospect_channel_lvl', 'prospect_name',
				'prospect_channel_cnt','juge_inter_cnt','juge_inter_success_cnt', 'expouse_cnt','buy_cnt', 'cps_cost', 
				'prospect_channel_cvr', 'prospect_channel_cac','prospect_channel_incr_cvr', 'prospect_channel_incr_p10k','prospect_channel_incr_pct', 'prospect_channel_incr_cac',
				'juge_channel_inter_rate', 'juge_channel_inter_success_rate','juge_task_level','juge_group_level', 'juge_inter_attenuation_para', 'juge_cate_conflict_para',
				'item_first_cate_cd', 'user_type']
				
	result_df =result_df[table_cols]
	double_cols = [
	'prospect_channel_cnt',
	'juge_inter_cnt',
	'juge_inter_success_cnt',
	'expouse_cnt',
	'buy_cnt',
	'cps_cost', 
	'prospect_channel_cvr',
	'prospect_channel_cac',
	'prospect_channel_incr_cvr',
	'prospect_channel_incr_p10k',
	'prospect_channel_incr_pct',
	'prospect_channel_incr_cac',
	'juge_channel_inter_rate',
	'juge_channel_inter_success_rate',
	'juge_task_level',
	'juge_group_level',
	'juge_inter_attenuation_para',
	'juge_cate_conflict_para']

	result_df[double_cols] = result_df[double_cols].fillna(0.0).round(4).astype(float)
	
	print("="*10,"check result_df")
	check_list = [check_data(result_df)]
	if any(check_list):
		print("[result_df] is null?",check_list)
		sys.exit()
	else:
		print("result_df is good")


	## 建表
	result_df =result_df.replace(np.NaN, '')
	print(result_df.columns)
	print('111')
	result_date = spark.createDataFrame(result_df)
	print(result_date.head(2))
	print('222')
	result_date.createOrReplaceTempView('cate_atomic_plan_basic_da')
	print('333')

	table_name = 'app.app_yhzz_juge_cate_atomic_plan_basic_da'
	spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
	spark.sql("""
	INSERT OVERWRITE TABLE app.app_yhzz_juge_cate_atomic_plan_basic_da PARTITION (dt='{part_dt}',item_first_cate_cd,user_type)
		select
		*
		from
		cate_atomic_plan_basic_da
	""".format(part_dt=part_dt))

