    SELECT
        user.dt,
        user.user_log_acct,
        user.my_hash_code,
        user.month_hash,
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
        
        user.is_malice_user,
        
        -- 目标登录次数
        COALESCE(grid.lower_lift_login_cnt,0) as lower_lift_login_cnt,
        COALESCE(user.natural_pred_login_cnt_rest,0) as natural_pred_login_cnt_rest,
        COALESCE(user.natural_pred_login_cnt_n30d,0) as natural_pred_login_cnt_n30d,
        
        -- 自然相关
        COALESCE(user.natural_login_cnt_last_month,0) as natural_login_cnt_last_month,
        COALESCE(grid.natural_max_login_cnt,0) as natural_max_login_cnt,
        COALESCE(grid.natural_login_cnt_per_month,0) as natural_login_cnt_per_month,
        COALESCE(grid.natural_quality_coeff,0) as natural_quality_coeff,
        COALESCE(grid.natural_login_cnt_per_dau,0) as natural_login_cnt_per_dau,

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
        COALESCE(grid.natural_login_cost,-3) as natural_login_cost,
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
        COALESCE(grid.normal_ctrl_login_cnt_per_dau,0.5)         as normal_ctrl_login_cnt_per_dau,
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
        case when grid.normal_ctrl_base_cnt <-100 then 0 else grid.normal_ctrl_base_cnt end as normal_ctrl_base_cnt,
        
        --- 大促放大系数
        promotion.com_cnt_rate,
        promotion.free_cnt_rate,
        promotion.strong_cnt_rate,
        promotion.normal_cnt_rate,
        promotion.nocontrol_cnt_rate

    FROM
    (
    SELECT
        dt,
        user_log_acct,
        hash_sub(concat(lower(trim(user_log_acct)),'{part_dt}')) as my_hash_code,
        hash_sub(concat(lower(trim(user_log_acct)),'{part_month}')) as month_hash,
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
        is_malice_user,
        
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
        -- case when clv_pred_30d <-100 then 0 else clv_mtd end as clv_pred_30d, --0

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
    AND priority_type in {user_type_list}
    )user

    JOIN

    (
    SELECT
        grid_name_1st,
        
        --阶跃下限
        max(case when lower_lift_login_cnt <-100 then 0 else lower_lift_login_cnt end) as lower_lift_login_cnt,
             

        --渠道上限
        max(case when natural_max_login_cnt <-100 then 2 else natural_max_login_cnt end )  as natural_max_login_cnt,
        max(case when com_max_login_cnt <-100 then 2 else com_max_login_cnt end )  as com_max_login_count,
        max(case when free_max_login_cnt <-100 then 2 else free_max_login_cnt end ) as free_max_login_count,
        max(case when strong_ctrl_max_login_count <-100 then 0 else strong_ctrl_max_login_count end ) as strong_ctrl_max_login_count,
        max(case when normal_ctrl_max_login_count <-100 then 0 else normal_ctrl_max_login_count end ) as normal_ctrl_max_login_count,
        max(case when no_ctrl_max_login_count <-100 then 0 else no_ctrl_max_login_count end ) as no_ctrl_max_login_count,
        
        --渠道成本
        max(case when natural_login_cost <-100 then -3 else natural_login_cost end ) as  natural_login_cost,
        max(case when com_login_cost <-100 then -3 else (-1 * com_login_cost) end ) as  com_login_cost,
        max(case when free_login_cost <-100 then 0 else free_login_cost end ) as free_login_cost,
        max(case when no_ctrl_login_cost <-100 then 0.8 else no_ctrl_login_cost end ) as no_ctrl_login_cost,
        max(case when strong_ctrl_login_cost <-100 then 0.6 else strong_ctrl_login_cost end ) as strong_ctrl_login_cost,
        max(case when normal_login_cost <-100 then 0.9 else normal_login_cost end ) as normal_login_cost,

        --渠道成本质量因子
        
        max(case when natural_quality_coeff <-100 then 1 else natural_quality_coeff end ) as natural_quality_coeff,
        max(case when com_quality_coeff <-100 then 0.28 else com_quality_coeff end ) as com_quality_coeff,
        max(case when free_quality_coeff <-100 then 0.64 else free_quality_coeff end ) as free_quality_coeff,
        max(case when strong_ctrl_quality_coeff <-100 then 2.2 else strong_ctrl_quality_coeff end ) as strong_ctrl_quality_coeff,
        max(case when normal_ctrl_quality_coeff <-100 then 0.25 else normal_ctrl_quality_coeff end ) as normal_ctrl_quality_coeff,
        max(case when no_ctrl_quality_coeff <-100 then 0.88 else no_ctrl_quality_coeff end ) as no_ctrl_quality_coeff,
        
        --渠道dau映射
        
        max(case when natural_login_cnt_per_dau <-100 then 0.478 else natural_login_cnt_per_dau end ) as natural_login_cnt_per_dau,
        max(case when com_login_cnt_per_dau <-100 then 0.460 else com_login_cnt_per_dau end ) as com_login_cnt_per_dau,
        max(case when free_login_cnt_per_dau <-100 then 0.462 else free_login_cnt_per_dau end ) as free_login_cnt_per_dau,
        max(case when no_ctrl_login_cnt_per_dau <-100 then 0.434 else no_ctrl_login_cnt_per_dau end ) as no_ctrl_login_cnt_per_dau,
        max(case when strong_ctrl_login_cnt_per_dau <-100 then 0.596 else strong_ctrl_login_cnt_per_dau end ) as strong_ctrl_login_cnt_per_dau,
        max(case when normal_login_cnt_per_dau <-100 then 0.507 else normal_login_cnt_per_dau end ) as normal_ctrl_login_cnt_per_dau,
         --dau映射
        max(case when total_login_dau_ratio <-100 then 0.8 else total_login_dau_ratio end) as total_login_dau_ratio,

        -- 格子用户数& 渠道月均引流 for 保量
        max(user_cnt) as user_cnt,
        max(natural_login_cnt_per_month) as natural_login_cnt_per_month,
        max(com_login_cnt_per_month) as com_login_cnt_per_month,
        max(free_login_cnt_per_month) as free_login_cnt_per_month,
        max(no_ctrl_login_cnt_per_month) as no_ctrl_login_cnt_per_month,
        max(strong_ctrl_login_cnt_per_month) as strong_ctrl_login_cnt_per_month,
        max(normal_ctrl_login_cnt_per_month) as normal_ctrl_login_cnt_per_month,
        max(cast(natural_login_cnt_per_month as  double)/user_cnt)         as natural_base_cnt,
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
    join
    (
        SELECT
            tag,
            natural_cnt_rate,
            com_cnt_rate,
            free_cnt_rate,
            strong_cnt_rate,
            normal_cnt_rate,
            nocontrol_cnt_rate,
            total_cnt_rate,
            natural_dau_rate,
            com_dau_rate,
            free_dau_rate,
            strong_dau_rate,
            normal_ctrl_dau,
            nocontrol_dau_rate,
            total_dau_rate
        FROM
            app.app_yhzz_temp_datacoefficient_bigpromotion
        WHERE
            dt = '2021-05-31'
           -- AND version = 'grid' --格子粒度系数 tag里面是格子名称
            AND  version = 'people' --人群粒度系数 tag里面是人群标识
   
    --    user.grid_name_1st = promotion.tag
    )promotion on  user.priority_type = promotion.tag    