sparse_features = [
 'gender',
 'allocate_strategy',
 'reserve_type',
 'customer_type',
 'is_tmktocp', 
 'company_type_id',
 'is_follow',
 'is_contact',
 'city_type',
 'driver_source',
 'is_rent_car',
 'spot_name',
 'age',
 'age_level',
 'passenger_phone_model',
 'purchase_power_level',
 'carlife_is_30d_ershouche',
 'psgr_age_level',
 'psgr_is_valet_driver',
 'psgr_is_carshare_rent_competitive_user',
 'carshare_is_carrent_ck',
 'psgr_is_rent_competitive_app_user',
 'is_credit_manhattan',
 'sub_channel_classify_1',
 'channel_id_1',

]
# 类别特征（multi-value）
sparse_features_multi_value = [

]
# 需要丢弃的特征，如label
drop_features = [
'driver_id',
'allocate_time',
'label',
'source_auditpass',
'source_id_auditpass',

]
# 需要做分位数阶段的特征
need_precentceil_feature = [
    
]
# 需要做分桶的特征
need_bin_feature = [

]
# 需要使用log10p函数的特征
need_log10_feature = [
 'car_age_day_cnt'  

]
# 需要使用ln函数的特征
need_ln_feature = [
'follow_cnt7',
'follow_cnt14',
'follow_cnt28',
'allocate_cnt',
'reserve_count',
'passenger_potential_driver_probability',
'psgr_order_to_competitive_store_90d',
'psgr_rent_competitive_user_app_num',
'psgr_car_info_competitive_user_app_num',
'psgr_install_borrow_money_app_num',
'psgr_install_short_rent_house_app_num',
'psgr_has_car_probability',
'consume_ability' 
]


# task1 features(因为是共享特征库，只写要去掉的即可)
task1_features = [
]
# task2 features
task2_features = [
    
]
# target
targets = ['label','label']

