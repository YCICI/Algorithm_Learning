
udf_func_val_list = ['dt',
                'month_clv_threshold',
                'year_clv_threshold',
                'channel_max_adjust',
                'channel_preference',
                'a1_threshold',
                'priority_type',
                'my_hash_code',
                'lower_lift_login_cnt',
                'natural_pred_login_cnt_rest',
                'login_cnt_mtd',
                'natural_login_cnt_mtd',
                'no_ctrl_login_cnt_mtd',
                'is_acce_by_com',
                'is_acce_by_free',
                'is_acce_by_strong_ctrl',
                'is_acce_by_normal_ctrl',
                'is_acce_by_no_ctrl',
                'com_max_login_count',
                'free_max_login_count',
                'strong_ctrl_max_login_count',
                'normal_ctrl_max_login_count',
                'no_ctrl_max_login_count',
                'com_login_cost',
                'free_login_cost',
                'strong_ctrl_login_cost',
                'normal_login_cost',
                'no_ctrl_login_cost',
                'com_quality_coeff',
                'free_quality_coeff',
                'strong_ctrl_quality_coeff',
                'normal_ctrl_quality_coeff',
                'no_ctrl_quality_coeff',
                'com_login_cnt_per_dau',
                'free_login_cnt_per_dau',
                'strong_ctrl_login_cnt_per_dau',
                'normal_ctrl_login_cnt_per_dau',
                'no_ctrl_login_cnt_per_dau',
                'clv_pred_1m',
                'gmv_year',
                'login_cnt_last_month',
                'natural_login_cnt_last_month',
                'com_login_cnt_last_month',
                'free_login_cnt_last_month',
                'strong_ctrl_login_cnt_last_month',
                'normal_ctrl_login_cnt_last_month',
                'no_ctrl_login_cnt_last_month',
                'com_login_cnt_mtd',
                'free_login_cnt_mtd',
                'strong_ctrl_login_cnt_mtd',
                'normal_ctrl_login_cnt_mtd',
                'month_hash', 
                'original_class', 
                'original_class_tg', 
                'original_class_sk', 
                'original_class_jzt',
                'user_class',
                'com_cnt_rate', 
                'free_cnt_rate', 
                'strong_cnt_rate', 
                'normal_cnt_rate', 
                'nocontrol_cnt_rate']

table_cols = ['user_log_acct','user_life_cycle_type_1st','model_a_1st','model_b_1st','goal_group_1st','annual_clv_1st','grid_name_1st','clv_pred_1m',
            'model_a','model_b','model_c','model_l','user_life_cycle_type',
            'login_cnt_mtd','natural_login_cnt_mtd','com_login_cnt_mtd','free_login_cnt_mtd',
            'strong_ctrl_login_cnt_mtd','normal_ctrl_login_cnt_mtd','no_ctrl_login_cnt_mtd',
            'com_user_last_month_login_cnt','free_user_last_month_login_cnt',
            'strong_ctrl_user_last_month_login_cnt','normal_ctrl_user_last_month_login_cnt',
            'no_ctrl_user_last_month_login_cnt','com_user_base_guarteed_cnt','free_user_base_guarteed_cnt',
            'strong_ctrl_user_base_guarteed_cnt','normal_ctrl_user_base_guarteed_cnt','no_ctrl_user_base_guarteed_cnt',
            'com_user_acce_flag','free_user_acce_flag',
            'strong_ctrl_user_acce_flag','normal_ctrl_user_acce_flag','no_ctrl_user_acce_flag',
            'com_grid_max_login_cnt','free_grid_max_login_cnt',
            'strong_ctrl_grid_max_login_cnt','normal_ctrl_grid_max_login_cnt','no_ctrl_grid_max_login_cnt',
            'com_grid_last_month_total_cnt','free_grid_last_month_total_cnt','strong_ctrl_grid_last_month_total_cnt',
            'normal_ctrl_grid_last_month_total_cnt','no_ctrl_grid_last_month_total_cnt',
            'com_grid_quality_coeff','free_grid_quality_coeff','strong_ctrl_grid_quality_coeff',
            'normal_ctrl_grid_quality_coeff','no_ctrl_grid_quality_coeff',  
            'com_grid_login_cnt_per_dau','free_grid_login_cnt_per_dau','strong_ctrl_grid_login_cnt_per_dau',
            'normal_grid_login_cnt_per_dau','no_ctrl_grid_login_cnt_per_dau',
            'com_grid_last_login_cost','free_grid_last_login_cost','strong_ctrl_grid_last_login_cost',
            'normal_ctrl_grid_last_login_cost','no_ctrl_grid_last_login_cost',
            'algo_total_info','natural_pred_login_cnt_rest',
            'com_login_cnt','free_login_cnt','strong_ctrl_login_cnt','normal_ctrl_login_cnt','no_ctrl_login_cnt',
            'priority_level','pred_com_dau','pred_free_dau','pred_strong_ctrl_dau','pred_normal_ctrl_dau','pred_no_ctrl_dau',
            'total_login_dau_ratio','pred_total_dau','is_solvable','end_time','is_malice_user','pred_natural_dau',
            'natural_user_last_month_login_cnt', 'natural_grid_max_login_cnt' ,'natural_grid_last_month_total_cnt', 'natural_grid_quality_coeff', 'natural_grid_login_cnt_per_dau','natural_last_login_cost', 
            'algo_com_login_cnt','algo_free_login_cnt','algo_strong_ctrl_login_cnt','algo_normal_ctrl_login_cnt','algo_no_ctrl_login_cnt','plan_flag',
            'priority_type'] # 修正 算法中间值

cols_dict = {'natural_login_cnt_last_month':'natural_user_last_month_login_cnt',\
                        'com_login_cnt_last_month':'com_user_last_month_login_cnt',\
                        'free_login_cnt_last_month':'free_user_last_month_login_cnt',\
                        'strong_ctrl_login_cnt_last_month':'strong_ctrl_user_last_month_login_cnt',\
                        'normal_ctrl_login_cnt_last_month':'normal_ctrl_user_last_month_login_cnt',\
                        'no_ctrl_login_cnt_last_month':'no_ctrl_user_last_month_login_cnt',\
                        
                        'is_guaranteed_by_com':'com_user_base_guarteed_cnt',\
                        'is_guaranteed_by_free':'free_user_base_guarteed_cnt',\
                        'is_guaranteed_by_strong_ctrl':'strong_ctrl_user_base_guarteed_cnt',\
                        'is_guaranteed_by_normal_ctrl':'normal_ctrl_user_base_guarteed_cnt',\
                        'is_guaranteed_by_no_ctrl':'no_ctrl_user_base_guarteed_cnt',\
                        'is_acce_by_com':'com_user_acce_flag',\
                        'is_acce_by_free':'free_user_acce_flag',\
                        'is_acce_by_strong_ctrl':'strong_ctrl_user_acce_flag',\
                        'is_acce_by_normal_ctrl':'normal_ctrl_user_acce_flag',\
                        'is_acce_by_no_ctrl':'no_ctrl_user_acce_flag',\
                            
                        'natural_max_login_cnt':'natural_grid_max_login_cnt',\
                        'com_max_login_count':'com_grid_max_login_cnt',\
                        'free_max_login_count':'free_grid_max_login_cnt',\
                        'strong_ctrl_max_login_count':'strong_ctrl_grid_max_login_cnt',\
                        'normal_ctrl_max_login_count':'normal_ctrl_grid_max_login_cnt',\
                        'no_ctrl_max_login_count':'no_ctrl_grid_max_login_cnt',\
                            
                        'natural_login_cnt_per_month':'natural_grid_last_month_total_cnt',\
                        'com_login_cnt_per_month':'com_grid_last_month_total_cnt',\
                        'free_login_cnt_per_month':'free_grid_last_month_total_cnt',\
                        'strong_ctrl_login_cnt_per_month':'strong_ctrl_grid_last_month_total_cnt',\
                        'normal_ctrl_login_cnt_per_month':'normal_ctrl_grid_last_month_total_cnt',\
                        'no_ctrl_login_cnt_per_month':'no_ctrl_grid_last_month_total_cnt',\
                            
                        'natural_login_cost':'natural_last_login_cost',\
                        'com_login_cost':'com_grid_last_login_cost',\
                        'free_login_cost':'free_grid_last_login_cost',\
                        'strong_ctrl_login_cost':'strong_ctrl_grid_last_login_cost',\
                        'normal_login_cost':'normal_ctrl_grid_last_login_cost',\
                        'no_ctrl_login_cost':'no_ctrl_grid_last_login_cost',\
                            
                        'natural_quality_coeff':'natural_grid_quality_coeff',\
                        'com_quality_coeff':'com_grid_quality_coeff',\
                        'free_quality_coeff':'free_grid_quality_coeff',\
                        'strong_ctrl_quality_coeff':'strong_ctrl_grid_quality_coeff',\
                        'normal_ctrl_quality_coeff':'normal_ctrl_grid_quality_coeff',\
                        'no_ctrl_quality_coeff':'no_ctrl_grid_quality_coeff',\
                        
                        'natural_login_cnt_per_dau':'natural_grid_login_cnt_per_dau',\
                        'com_login_cnt_per_dau':'com_grid_login_cnt_per_dau',\
                        'free_login_cnt_per_dau':'free_grid_login_cnt_per_dau',\
                        'strong_ctrl_login_cnt_per_dau':'strong_ctrl_grid_login_cnt_per_dau',\
                        'normal_ctrl_login_cnt_per_dau':'normal_grid_login_cnt_per_dau',\
                        'no_ctrl_login_cnt_per_dau':'no_ctrl_grid_login_cnt_per_dau'}

resource_pra = {'gy_new':
            {
            0:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            1:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            2:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            3:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            4:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            5:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.7},
            6:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.4},
            7:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.3},
            8:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.0},
            9:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.0},
            },
            'high_active':
            {
            0:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            1:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            2:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            3:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            4:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            5:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            6:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            7:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            8:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            9:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            },
            'low_active':
            {
            0:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            1:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            2:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            3:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':0.0},
            4:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.0},
            5:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':1.0},
            6:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':2.0},
            7:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':2.0},
            8:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':3.0},
            9:{'com':1.0,'free':1.0,'strong_ctrl':1.0,'normal_ctrl':3.0},
            }
        }

