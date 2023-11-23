sparse_features = [


    'gender',
    'is_gulf_driver',#
    'is_fast_driver',
    'driver_join_model',
    'grade_id',
    'role'

]
# 类别特征（multi-value）
sparse_features_multi_value = [

]

# 需要丢弃的特征，如label
drop_features = [
'pid','dt','ltv_label','cvr_label'
]

# 需要做分位数阶段的特征
need_precentceil_feature = [
#加油券使用率
'gas_coupon_rate_total','gas_coupon_rate_30d','gas_coupon_rate_60d',
'gas_coupon_rate_90d','gas_coupon_rate_180d'

]
# 需要做分桶的特征
need_bin_feature =['age',
'gas_coupon_rate_total','gas_coupon_rate_30d','gas_coupon_rate_60d',
'gas_coupon_rate_90d','gas_coupon_rate_180d',
]
# 需要使用log10p函数的特征
need_log10_feature = [
'driver_first_finish_day',
'first_gas_duration','last_gas_duration',
#拉单量bigint 拉单里程double 在线时间double
'driver_order_ct_total','driver_mileage_total','driver_online_time_total',
'driver_order_ct_7d','driver_mileage_7d','driver_online_time_7d',
'driver_order_ct_14d','driver_mileage_14d','driver_online_time_14d',
'driver_order_ct_30d','driver_mileage_30d','driver_online_time_30d',
'driver_order_ct_60d','driver_mileage_60d','driver_online_time_60d',
'driver_order_ct_90d','driver_mileage_90d','driver_online_time_90d',
'driver_order_ct_180d','driver_mileage_180d','driver_online_time_180d',
#加油单量bigint
'gas_order_cnt_30d','gas_order_cnt_90d','gas_order_cnt_180d',
'gas_store_cnt_30d','gas_store_cnt_90d','gas_store_cnt_180d',
#加单量／拉单里程
'gas_order_saturation_30d','gas_order_saturation_60d','gas_order_saturation_90d',
'gas_order_saturation_180d'
]

# 需要使用ln函数的特征
need_ln_feature = [
'driver_last_finish_day','gas_cnt','gas_store_cnt',

'avg_station_cnt_7','avg_station_cnt_14','avg_station_cnt_30',
'avg_station_cnt_cmonth','avg_station_cnt_lmonth','avg_station_cnt_llmonth',


'd_store_gas_cnt_max_pct',
'gas_order_saturation_1d','gas_order_saturation_1w','gas_order_saturation_1m',
'order_finish_distance_1d','order_finish_distance_1w','order_finish_distance_1m',

#点击转化率

'gas_station_detail_ctr_7','gas_station_detail_ctr_14',
'gas_ratio_7','gas_ratio_14',

'broadcast_ctr_1','push_ctr_1','msg_ctr_1',
'broadcast_cvr_1','push_cvr_1','msg_cvr_1',

'broadcast_ctr_7','push_ctr_7','msg_ctr_7',
'broadcast_cvr_7','push_cvr_7','msg_cvr_7',

'broadcast_ctr_14','push_ctr_14','msg_ctr_14',
'broadcast_cvr_14','push_cvr_14','msg_cvr_14',

'broadcast_ctr_30','push_ctr_30','msg_ctr_30',
'broadcast_cvr_30','push_cvr_30','msg_cvr_30',

'broadcast_ctr_all','push_ctr_all','msg_ctr_all',
'broadcast_cvr_all','push_cvr_all','msg_cvr_all',

'gas_push_reach_open_rate_90d','gas_push_open_count_90d',
'gas_push_reach_open_rate_60d','gas_push_open_count_60d',
'gas_push_reach_open_rate_30d','gas_push_open_count_30d',

'gas_broad_reach_open_rate_90d','gas_broad_open_count_90d',
'gas_broad_reach_open_rate_60d','gas_broad_open_count_60d',
'gas_broad_reach_open_rate_30d','gas_broad_open_count_30d',

'gas_push_reach_open_rate_14d','gas_push_open_count_14d',
'gas_push_reach_open_rate_7d','gas_push_open_count_7d',

'gas_broad_reach_open_rate_14d','gas_broad_open_count_14d',
'gas_broad_reach_open_rate_7d','gas_broad_open_count_7d',  

'res_gas_order_cnt'

]

# task1 features(因为是共享特征库，只写要去掉的即可)
# task1 = ltv
task1_features = [
'gas_station_detail_ctr_7',
'gas_ratio_7',
'broadcast_ctr_1',
'push_ctr_1',
'msg_ctr_1',
'broadcast_cvr_1',
'push_cvr_1',
'msg_cvr_1',
'broadcast_ctr_7',
'push_ctr_7',
'msg_ctr_7',
'broadcast_cvr_7',
'push_cvr_7',
'msg_cvr_7',
'driver_order_ct_7d',
'driver_mileage_7d',
'driver_online_time_7d',
'driver_order_ct_14d',
'driver_mileage_14d',
'driver_online_time_14d',
'gas_push_reach_open_rate_7d',
'gas_push_open_count_7d',
'gas_push_reach_open_rate_14d',
'gas_push_open_count_14d',
'gas_broad_reach_open_rate_7d',
'gas_broad_open_count_7d',
'gas_broad_reach_open_rate_14d',
'gas_broad_open_count_14d',
'res_gas_order_cnt',
'avg_station_cnt_7',
'avg_station_cnt_14',
'gas_order_saturation_1d',
'order_finish_distance_1d',
'order_finish_distance_1w',
'gas_order_saturation_1w',
]
# task2 features
#task2 =cvr
task2_features = [
'driver_order_ct_60d',
'driver_mileage_60d',
'driver_online_time_60d',
'driver_order_ct_90d',
'driver_mileage_90d',
'driver_online_time_90d',
'driver_order_ct_180d',
'driver_mileage_180d',
'driver_online_time_180d',
'gas_order_cnt_90d',
'gas_order_cnt_180d',
'gas_store_cnt_60d',
'gas_store_cnt_90d',
'gas_store_cnt_180d',
'gas_coupon_rate_60d',
'gas_coupon_rate_90d',
'gas_coupon_rate_180d',
'gas_order_saturation_60d',
'gas_order_saturation_90d',
'gas_order_saturation_180d',
'gas_push_reach_open_rate_90d',
'gas_push_open_count_90d',
'gas_push_reach_open_rate_60d',
'gas_push_open_count_60d',
'gas_push_reach_open_rate_30d',
'gas_push_open_count_30d',
'gas_broad_reach_open_rate_90d',
'gas_broad_open_count_90d',
'gas_broad_reach_open_rate_60d',
'gas_broad_open_count_60d',
'avg_station_cnt_cmonth',
'avg_station_cnt_lmonth',
'avg_station_cnt_llmonth',
'order_finish_distance_1m',
'gas_order_saturation_1m'
]

# target
targets = ['ltv_label','cvr_label']
