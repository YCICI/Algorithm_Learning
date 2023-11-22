--获取昨日方案
SELECT
    interim.user_log_acct,
    user_life_cycle_type_1st,
    model_a_1st, 
    model_b_1st,
    goal_group_1st,
    annual_clv_1st,
    interim.grid_name_1st,
    clv_pred_1m,
    model_a,
    model_b,
    model_c,
    model_l,
    user_life_cycle_type,
    user.login_cnt_mtd,
    user.natural_login_cnt_mtd,
    user.com_login_cnt_mtd,
    user.free_login_cnt_mtd,
    user.strong_ctrl_login_cnt_mtd,
    user.normal_ctrl_login_cnt_mtd,
    user.no_ctrl_login_cnt_mtd,
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
    natural_pred_login_cnt_rest,
    case when (algo_com_login_cnt - user.com_login_cnt_mtd)>=0 then (algo_com_login_cnt - user.com_login_cnt_mtd)
                else 0 end as com_login_cnt,
    case when (algo_free_login_cnt - user.free_login_cnt_mtd)>=0 then (algo_free_login_cnt - user.free_login_cnt_mtd)
        else 0 end as free_login_cnt,
    case when (algo_strong_ctrl_login_cnt - user.strong_ctrl_login_cnt_mtd)>=0 then (algo_strong_ctrl_login_cnt - user.strong_ctrl_login_cnt_mtd)
        else 0 end as strong_ctrl_login_cnt,
    case when (algo_normal_ctrl_login_cnt - user.normal_ctrl_login_cnt_mtd)>=0 then (algo_normal_ctrl_login_cnt - user.normal_ctrl_login_cnt_mtd)
        else 0 end as normal_ctrl_login_cnt,
    case when (algo_no_ctrl_login_cnt - user.no_ctrl_login_cnt_mtd)>=0 then (algo_no_ctrl_login_cnt - user.no_ctrl_login_cnt_mtd)
        else 0 end as no_ctrl_login_cnt,
    priority_level,
    --todo dau校验
    case when (algo_com_login_cnt - user.com_login_cnt_mtd)>=0 then ((algo_com_login_cnt - user.com_login_cnt_mtd) * com_grid_login_cnt_per_dau)
                else 0 end as pred_com_dau,
    case when (algo_free_login_cnt - user.free_login_cnt_mtd)>=0 then ((algo_free_login_cnt - user.free_login_cnt_mtd) * free_grid_login_cnt_per_dau)
        else 0 end as pred_free_dau,
    case when (algo_strong_ctrl_login_cnt - user.strong_ctrl_login_cnt_mtd)>=0 then ((algo_strong_ctrl_login_cnt - user.strong_ctrl_login_cnt_mtd) * strong_ctrl_grid_login_cnt_per_dau )
        else 0 end as pred_strong_ctrl_dau,
    case when (algo_normal_ctrl_login_cnt - user.normal_ctrl_login_cnt_mtd)>=0 then ((algo_normal_ctrl_login_cnt - user.normal_ctrl_login_cnt_mtd) * normal_grid_login_cnt_per_dau)
        else 0 end as pred_normal_ctrl_dau,
    case when (algo_no_ctrl_login_cnt - user.no_ctrl_login_cnt_mtd)>=0 then ((algo_no_ctrl_login_cnt - user.no_ctrl_login_cnt_mtd) * no_ctrl_grid_login_cnt_per_dau)
        else 0 end as pred_no_ctrl_dau,
    --todo dau校验
    total_login_dau_ratio,
    total_login_dau_ratio * ((com_login_cnt * com_grid_login_cnt_per_dau) + ( free_login_cnt * free_grid_login_cnt_per_dau ) + ( strong_ctrl_login_cnt * strong_ctrl_grid_login_cnt_per_dau ) + ( normal_ctrl_login_cnt * normal_grid_login_cnt_per_dau )  + ( no_ctrl_login_cnt * no_ctrl_grid_login_cnt_per_dau )) as pred_total_dau,
    case when (com_login_cnt + free_login_cnt + strong_ctrl_login_cnt + normal_ctrl_login_cnt + no_ctrl_login_cnt)>= (lower_lift_login_cnt-login_cnt_mtd) then 1
        else 0 end as is_solvable,
    end_time,
    is_malice_user,
    pred_natural_dau,
    priority_type,
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
    algo_no_ctrl_login_cnt
FROM

(
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
        natural_pred_login_cnt_rest,
        com_login_cnt,
        free_login_cnt,
        strong_ctrl_login_cnt,
        normal_ctrl_login_cnt,
        no_ctrl_login_cnt,
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
        priority_type,
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
        algo_no_ctrl_login_cnt
    FROM 
        app.app_yhzz_umc_algo_pin_interim
    where 
        dt = '2022-06-17' --T+2 方案
        and dp ='low'
        and priority_type in 'A1'
    )interim 
left join
    (
        select
            user_log_acct,
            --当月引流登端次数
            case when login_cnt_mtd<-100 then 0
                else login_cnt_mtd end as login_cnt_mtd,
            case when natural_login_cnt_mtd <-100 then 0 else natural_login_cnt_mtd end as natural_login_cnt_mtd,
            case when com_login_cnt_mtd <-100 then 0 else com_login_cnt_mtd end as com_login_cnt_mtd,
            case when free_login_cnt_mtd <-100 then 0 else free_login_cnt_mtd end as free_login_cnt_mtd,
            case when strong_ctrl_login_cnt_mtd <-100 then 0 else strong_ctrl_login_cnt_mtd end as strong_ctrl_login_cnt_mtd,
            case when normal_ctrl_login_cnt_mtd <-100 then 0 else normal_ctrl_login_cnt_mtd end as normal_ctrl_login_cnt_mtd,
            case when no_ctrl_login_cnt_mtd <-100 then 0 else no_ctrl_login_cnt_mtd end as no_ctrl_login_cnt_mtd
        FROM
            app.app_yhzz_umc_unit_user
        where
            dt = '{part_dt}' --T+1已完成
        
    )user on interim.user_log_acct = user.user_log_acct
left join
    (
    select
    -- 目标登录次数
        grid_name_1st,
        COALESCE(lower_lift_login_cnt,0) as lower_lift_login_cnt

    FROM
        app.app_yhzz_umc_unit_grid
    WHERE
        dt = '{part_dt}' --T+1目标
        AND grid_name_1st is not null
        AND is_grid_valid = 1
    )grid on interim.grid_name_1st  = grid.grid_name_1st