---- 广义新底层数据
SELECT
	new.dt,
	new.user_log_acct,
	new.my_hash_code,
	new.month_hash,
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
	CASE WHEN (user.user_log_acct is not null ) then 'gy_new_1'
	     ELSE 'gy_new_2' end as priority_type,
	COALESCE(user.is_malice_user,0) AS is_malice_user,

	-- 目标登录次数
	COALESCE(grid.lower_lift_login_cnt, 0) AS lower_lift_login_cnt,
	COALESCE(user.natural_pred_login_cnt_rest, 0) AS natural_pred_login_cnt_rest,
	--COALESCE(tmp.natural_pred_login_cnt_rest,0) as natural_pred_login_cnt_rest,
	COALESCE(user.natural_pred_login_cnt_n30d, 0) AS natural_pred_login_cnt_n30d,
	-- 自然相关
	COALESCE(user.natural_login_cnt_last_month, 0) AS natural_login_cnt_last_month,
	COALESCE(grid.natural_max_login_cnt, 0) AS natural_max_login_cnt,
	COALESCE(grid.natural_login_cnt_per_month, 0) AS natural_login_cnt_per_month,
	COALESCE(grid.natural_quality_coeff, 0) AS natural_quality_coeff,
	COALESCE(grid.natural_login_cnt_per_dau, 0) AS natural_login_cnt_per_dau,
	--当月引流登端次数
	COALESCE(user.login_cnt_mtd, 0) AS login_cnt_mtd,
	COALESCE(user.natural_login_cnt_mtd,0) AS  natural_login_cnt_mtd,
	COALESCE(user.com_login_cnt_mtd,0) AS com_login_cnt_mtd ,
	COALESCE(user.free_login_cnt_mtd,0) AS free_login_cnt_mtd ,
	COALESCE(user.strong_ctrl_login_cnt_mtd,0) AS strong_ctrl_login_cnt_mtd ,
	COALESCE(user.normal_ctrl_login_cnt_mtd,0) AS  normal_ctrl_login_cnt_mtd,
	COALESCE(user.no_ctrl_login_cnt_mtd,0) AS  no_ctrl_login_cnt_mtd,

	--渠道是否可达
	COALESCE(user.is_acce_by_com,0) as is_acce_by_com,
	COALESCE(user.is_acce_by_free,0) as is_acce_by_free,
	COALESCE(user.is_acce_by_strong_ctrl,0) as is_acce_by_strong_ctrl,
	COALESCE(user.is_acce_by_normal_ctrl,0) as is_acce_by_normal_ctrl,
	COALESCE(user.is_acce_by_no_ctrl,0) as is_acce_by_no_ctrl,

	--clv
	COALESCE(user.clv_pred_1m,0) as clv_pred_1m,
	COALESCE(user.clv_mtd,0) as clv_mtd,

	--上月人均干预次数
	COALESCE(user.login_cnt_last_month, 0) AS login_cnt_last_month,
	COALESCE(user.com_login_cnt_last_month, grid.com_base_cnt, 0) AS com_login_cnt_last_month,
	COALESCE(user.free_login_cnt_last_month, grid.free_base_cnt, 0) AS free_login_cnt_last_month,
	COALESCE(user.strong_ctrl_login_cnt_last_month, grid.strong_ctrl_base_cnt, 0) AS strong_ctrl_login_cnt_last_month,
	COALESCE(user.normal_ctrl_login_cnt_last_month, grid.normal_ctrl_base_cnt, 0) AS normal_ctrl_login_cnt_last_month,    
	COALESCE(user.no_ctrl_login_cnt_last_month, grid.no_ctrl_base_cnt, 0) AS no_ctrl_login_cnt_last_month,    
	--gmv
	CAST(COALESCE(gmv_year, 0.0) AS DOUBLE) AS gmv_year,
	CAST(COALESCE(last_year_gmv, 0.0) AS DOUBLE) AS last_year_gmv,
	--渠道上限
	COALESCE(grid.com_max_login_count, 2) AS com_max_login_count,
	COALESCE(grid.free_max_login_count, 2) AS free_max_login_count,
	COALESCE(grid.strong_ctrl_max_login_count, 0) AS strong_ctrl_max_login_count,
	COALESCE(grid.normal_ctrl_max_login_count, 0) AS normal_ctrl_max_login_count,
	COALESCE(grid.no_ctrl_max_login_count, 0) AS no_ctrl_max_login_count,
	--渠道成本
	COALESCE(grid.natural_login_cost, -3 ) AS natural_login_cost,
	COALESCE(grid.com_login_cost, -3) AS com_login_cost,
	COALESCE(grid.free_login_cost, 0) AS free_login_cost,
	COALESCE(grid.no_ctrl_login_cost, 0.9) AS no_ctrl_login_cost,
	COALESCE(grid.strong_ctrl_login_cost, 0.8) AS strong_ctrl_login_cost,
	COALESCE(grid.normal_login_cost, 0.6) AS normal_login_cost,
	--渠道成本质量因子
	COALESCE(com_quality_coeff,1) AS  com_quality_coeff ,
	COALESCE(free_quality_coeff,1) AS  free_quality_coeff ,
	COALESCE(strong_ctrl_quality_coeff,1) AS  strong_ctrl_quality_coeff ,
	COALESCE(normal_ctrl_quality_coeff,1) AS  normal_ctrl_quality_coeff ,
	COALESCE(no_ctrl_quality_coeff,1) AS  no_ctrl_quality_coeff ,
	--渠道dau映射
	COALESCE(grid.com_login_cnt_per_dau, 0.5) AS com_login_cnt_per_dau,
	COALESCE(grid.free_login_cnt_per_dau, 0.5) AS free_login_cnt_per_dau,
	COALESCE(grid.no_ctrl_login_cnt_per_dau, 0.5) AS no_ctrl_login_cnt_per_dau,
	COALESCE(grid.strong_ctrl_login_cnt_per_dau, 0.5) AS strong_ctrl_login_cnt_per_dau,
	COALESCE(grid.normal_ctrl_login_cnt_per_dau, 0.5) AS normal_ctrl_login_cnt_per_dau,
	COALESCE(grid.total_login_dau_ratio,0.8) as total_login_dau_ratio,
	-- 格子用户数& 渠道月均引流 for 保量
	COALESCE(CASE
		WHEN grid.user_cnt < - 100
		THEN 1
		ELSE grid.user_cnt
	END,1) AS user_cnt,
	COALESCE(CASE
		WHEN grid.com_login_cnt_per_month < - 100
		THEN 1
		ELSE grid.com_login_cnt_per_month
	END,1) AS com_login_cnt_per_month,
	COALESCE(CASE
		WHEN grid.free_login_cnt_per_month < - 100
		THEN 1
		ELSE grid.free_login_cnt_per_month
	END,1) AS free_login_cnt_per_month,
	COALESCE(CASE
		WHEN grid.no_ctrl_login_cnt_per_month < - 100
		THEN 1
		ELSE grid.no_ctrl_login_cnt_per_month
	END,1) AS no_ctrl_login_cnt_per_month,
	COALESCE(CASE
		WHEN grid.strong_ctrl_login_cnt_per_month < - 100
		THEN 1
		ELSE grid.strong_ctrl_login_cnt_per_month
	END,1) AS strong_ctrl_login_cnt_per_month,
	COALESCE(CASE
		WHEN grid.normal_ctrl_login_cnt_per_month < - 100
		THEN 1
		ELSE grid.normal_ctrl_login_cnt_per_month
	END,1) AS normal_ctrl_login_cnt_per_month,
	-- 上月人均引流次数（按格子统计）
	CASE
		WHEN (grid.com_base_cnt < - 100) or (grid.com_base_cnt is NULL)
		THEN 0
		ELSE grid.com_base_cnt
	END AS com_base_cnt,
	CASE
		WHEN (grid.free_base_cnt < - 100) or (grid.free_base_cnt is NULL)
		THEN 0
		ELSE grid.free_base_cnt
	END AS free_base_cnt,
	CASE
		WHEN (grid.no_ctrl_base_cnt < - 100) or (grid.no_ctrl_base_cnt is NULL)
		THEN 0
		ELSE grid.no_ctrl_base_cnt
	END AS no_ctrl_base_cnt,
	CASE
		WHEN (grid.strong_ctrl_base_cnt < - 100) or (grid.strong_ctrl_base_cnt is NULL)
		THEN 0
		ELSE grid.strong_ctrl_base_cnt
	END AS strong_ctrl_base_cnt,
	CASE
		WHEN (grid.normal_ctrl_base_cnt < - 100) or (grid.normal_ctrl_base_cnt is NULL)
		THEN 0
		ELSE grid.normal_ctrl_base_cnt
	END AS normal_ctrl_base_cnt,
	    --- 大促放大系数
        COALESCE(promotion.com_cnt_rate, 1) as  com_cnt_rate,
        COALESCE(promotion.free_cnt_rate, 1) as  free_cnt_rate,
        COALESCE(promotion.strong_cnt_rate, 1) as  strong_cnt_rate,
        COALESCE(promotion.normal_cnt_rate, 1) as  normal_cnt_rate,
        COALESCE(promotion.nocontrol_cnt_rate, 1) as  nocontrol_cnt_rate
FROM
	(
    SELECT
	  a1.dt,
      a1.user_log_acct as user_log_acct,
      COALESCE(a2.grid_name,-1) as grid_name,
      COALESCE(a2.user_life_cycle_type,-1) as user_life_cycle_type,
	  COALESCE(a1.my_hash_code, 0) as my_hash_code,
	  COALESCE(a1.month_hash, 0) as month_hash
    FROM
    (
    SELECT
	    dt,
        user_log_acct,
		hash_sub(concat(lower(trim(user_log_acct)), '{part_dt}')) AS my_hash_code,
		hash_sub(concat(lower(trim(user_log_acct)), '{part_month}')) AS month_hash

    FROM
        app.app_yhzz_gy_new_user_selection
    WHERE
        dt = '{part_dt_3}'
	group by
	    dt,
        user_log_acct
    )a1
    LEFT JOIN
    (
    SELECT
        user_log_acct,
        grid_name,
        user_life_cycle_type,
        clv_level
    FROM
       app.app_yhzz_hubble_user_insight
    WHERE
       dt = '{part_dt_3}'
       AND clv_level >0 
    )a2 on a1.user_log_acct = a2.user_log_acct
)new
LEFT JOIN
	(
		SELECT
			dt,
			user_log_acct,

			--格子粒度
			user_life_cycle_type_1st,
			model_a_1st,
			model_b_1st,
			goal_group_1st,
			CASE
				WHEN annual_clv_1st IS NULL
				THEN '0'
				ELSE annual_clv_1st
			END AS annual_clv_1st,
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
			CASE
				WHEN natural_pred_login_cnt_rest < - 100
				THEN 0
				ELSE natural_pred_login_cnt_rest
			END AS natural_pred_login_cnt_rest,
			CASE
				WHEN natural_pred_login_cnt_n30d < - 100
				THEN 0
				ELSE natural_pred_login_cnt_n30d
			END AS natural_pred_login_cnt_n30d,
			--当月引流登端次数
			CASE
				WHEN login_cnt_mtd < - 100
				THEN 0
				ELSE login_cnt_mtd
			END AS login_cnt_mtd,
			CASE
				WHEN natural_login_cnt_mtd < - 100
				THEN 0
				ELSE natural_login_cnt_mtd
			END AS natural_login_cnt_mtd,
			CASE
				WHEN com_login_cnt_mtd < - 100
				THEN 0
				ELSE com_login_cnt_mtd
			END AS com_login_cnt_mtd,
			CASE
				WHEN free_login_cnt_mtd < - 100
				THEN 0
				ELSE free_login_cnt_mtd
			END AS free_login_cnt_mtd,
			CASE
				WHEN strong_ctrl_login_cnt_mtd < - 100
				THEN 0
				ELSE strong_ctrl_login_cnt_mtd
			END AS strong_ctrl_login_cnt_mtd,
			CASE
				WHEN normal_ctrl_login_cnt_mtd < - 100
				THEN 0
				ELSE normal_ctrl_login_cnt_mtd
			END AS normal_ctrl_login_cnt_mtd,
			CASE
				WHEN no_ctrl_login_cnt_mtd < - 100
				THEN 0
				ELSE no_ctrl_login_cnt_mtd
			END AS no_ctrl_login_cnt_mtd,
			--渠道是否可达
			CASE
				WHEN is_acce_by_com < - 100
				THEN 1
				ELSE is_acce_by_com
			END AS is_acce_by_com,
			CASE
				WHEN is_acce_by_free < - 100
				THEN 1
				ELSE is_acce_by_free
			END AS is_acce_by_free,
			CASE
				WHEN is_acce_by_strong_ctrl < - 100
				THEN 1
				ELSE is_acce_by_strong_ctrl
			END AS is_acce_by_strong_ctrl,
			CASE
				WHEN is_acce_by_normal_ctrl < - 100
				THEN 1
				ELSE is_acce_by_normal_ctrl
			END AS is_acce_by_normal_ctrl,
			CASE
				WHEN is_acce_by_no_ctrl < - 100
				THEN 1
				ELSE is_acce_by_no_ctrl
			END AS is_acce_by_no_ctrl,
			--clv
			CASE
				WHEN clv_pred_1m < - 100
				THEN 0
				ELSE clv_pred_1m
			END AS clv_pred_1m, --0
			CASE
				WHEN clv_mtd < - 100
				THEN 0
				ELSE clv_mtd
			END AS clv_mtd, --0
			-- case when clv_pred_30d <-100 then 0 else clv_mtd end as clv_pred_30d, --0
			--上月人均干预次数
			CASE
				WHEN login_cnt_last_month < - 100
				THEN 0
				ELSE login_cnt_last_month
			END AS login_cnt_last_month,
			CASE
				WHEN natural_login_cnt_last_month < - 100
				THEN 0
				ELSE natural_login_cnt_last_month
			END AS natural_login_cnt_last_month,
			CASE
				WHEN com_login_cnt_last_month < - 100
				THEN 0
				ELSE com_login_cnt_last_month
			END AS com_login_cnt_last_month,
			CASE
				WHEN free_login_cnt_last_month < - 100
				THEN 0
				ELSE free_login_cnt_last_month
			END AS free_login_cnt_last_month,
			CASE
				WHEN strong_ctrl_login_cnt_last_month < - 100
				THEN 0
				ELSE strong_ctrl_login_cnt_last_month
			END AS strong_ctrl_login_cnt_last_month,
			CASE
				WHEN normal_ctrl_login_cnt_last_month < - 100
				THEN 0
				ELSE normal_ctrl_login_cnt_last_month
			END AS normal_ctrl_login_cnt_last_month,
			CASE
				WHEN no_ctrl_login_cnt_last_month < - 100
				THEN 0
				ELSE no_ctrl_login_cnt_last_month
			END AS no_ctrl_login_cnt_last_month,
			CASE
				WHEN clv_year < - 100
				THEN 0
				ELSE clv_year
			END AS gmv_year,
			CASE
				WHEN last_year_gmv < - 100
				THEN 0
				ELSE last_year_gmv
			END AS last_year_gmv
		FROM
			app.app_yhzz_umc_unit_user
		WHERE
			dt = '{part_dt}'
	)
	USER
ON
	new.user_log_acct = user.user_log_acct

left JOIN
	(
		SELECT
			grid_name_1st,
			--阶跃下限
			MAX(
				CASE
					WHEN lower_lift_login_cnt < - 100
					THEN 0
					ELSE lower_lift_login_cnt
				END) AS lower_lift_login_cnt,
			--渠道上限
			MAX(
				CASE
					WHEN natural_max_login_cnt < - 100
					THEN 2
					ELSE natural_max_login_cnt
				END) AS natural_max_login_cnt,
			MAX(
				CASE
					WHEN com_max_login_cnt < - 100
					THEN 2
					ELSE com_max_login_cnt
				END) AS com_max_login_count,
			MAX(
				CASE
					WHEN free_max_login_cnt < - 100
					THEN 2
					ELSE free_max_login_cnt
				END) AS free_max_login_count,
			MAX(
				CASE
					WHEN strong_ctrl_max_login_count < - 100
					THEN 0
					ELSE strong_ctrl_max_login_count
				END) AS strong_ctrl_max_login_count,
			MAX(
				CASE
					WHEN normal_ctrl_max_login_count < - 100
					THEN 0
					ELSE normal_ctrl_max_login_count
				END) AS normal_ctrl_max_login_count,
			MAX(
				CASE
					WHEN no_ctrl_max_login_count < - 100
					THEN 0
					ELSE no_ctrl_max_login_count
				END) AS no_ctrl_max_login_count,
			--渠道成本
			MAX(
				CASE
					WHEN natural_login_cost < - 100
					THEN - 3
					ELSE natural_login_cost
				END) AS natural_login_cost,
			MAX(
				CASE
					WHEN com_login_cost < - 100
					THEN - 3
					ELSE( - 1 * com_login_cost)
				END) AS com_login_cost,
			MAX(
				CASE
					WHEN free_login_cost < - 100
					THEN 0
					ELSE free_login_cost
				END) AS free_login_cost,
			MAX(
				CASE
					WHEN no_ctrl_login_cost < - 100
					THEN 0.8
					ELSE no_ctrl_login_cost
				END) AS no_ctrl_login_cost,
			MAX(
				CASE
					WHEN strong_ctrl_login_cost < - 100
					THEN 0.6
					ELSE strong_ctrl_login_cost
				END) AS strong_ctrl_login_cost,
			MAX(
				CASE
					WHEN normal_login_cost < - 100
					THEN 0.9
					ELSE normal_login_cost
				END) AS normal_login_cost,
			--渠道成本质量因子
			MAX(
				CASE
					WHEN natural_quality_coeff < - 100
					THEN 1
					ELSE natural_quality_coeff
				END) AS natural_quality_coeff,
			MAX(
				CASE
					WHEN com_quality_coeff < - 100
					THEN 0.28
					ELSE com_quality_coeff
				END) AS com_quality_coeff,
			MAX(
				CASE
					WHEN free_quality_coeff < - 100
					THEN 0.64
					ELSE free_quality_coeff
				END) AS free_quality_coeff,
			MAX(
				CASE
					WHEN strong_ctrl_quality_coeff < - 100
					THEN 2.2
					ELSE strong_ctrl_quality_coeff
				END) AS strong_ctrl_quality_coeff,
			MAX(
				CASE
					WHEN normal_ctrl_quality_coeff < - 100
					THEN 0.25
					ELSE normal_ctrl_quality_coeff
				END) AS normal_ctrl_quality_coeff,
			MAX(
				CASE
					WHEN no_ctrl_quality_coeff < - 100
					THEN 0.88
					ELSE no_ctrl_quality_coeff
				END) AS no_ctrl_quality_coeff,
			--渠道dau映射
			MAX(
				CASE
					WHEN natural_login_cnt_per_dau < - 100
					THEN 0.478
					ELSE natural_login_cnt_per_dau
				END) AS natural_login_cnt_per_dau,
			MAX(
				CASE
					WHEN com_login_cnt_per_dau < - 100
					THEN 0.460
					ELSE com_login_cnt_per_dau
				END) AS com_login_cnt_per_dau,
			MAX(
				CASE
					WHEN free_login_cnt_per_dau < - 100
					THEN 0.462
					ELSE free_login_cnt_per_dau
				END) AS free_login_cnt_per_dau,
			MAX(
				CASE
					WHEN no_ctrl_login_cnt_per_dau < - 100
					THEN 0.434
					ELSE no_ctrl_login_cnt_per_dau
				END) AS no_ctrl_login_cnt_per_dau,
			MAX(
				CASE
					WHEN strong_ctrl_login_cnt_per_dau < - 100
					THEN 0.596
					ELSE strong_ctrl_login_cnt_per_dau
				END) AS strong_ctrl_login_cnt_per_dau,
			MAX(
				CASE
					WHEN normal_login_cnt_per_dau < - 100
					THEN 0.507
					ELSE normal_login_cnt_per_dau
				END) AS normal_ctrl_login_cnt_per_dau,
			--dau映射
			MAX(
				CASE
					WHEN total_login_dau_ratio < - 100
					THEN 0.8
					ELSE total_login_dau_ratio
				END) AS total_login_dau_ratio,
			-- 格子用户数& 渠道月均引流 for 保量
			MAX(user_cnt) AS user_cnt,
			MAX(natural_login_cnt_per_month) AS natural_login_cnt_per_month,
			MAX(com_login_cnt_per_month) AS com_login_cnt_per_month,
			MAX(free_login_cnt_per_month) AS free_login_cnt_per_month,
			MAX(no_ctrl_login_cnt_per_month) AS no_ctrl_login_cnt_per_month,
			MAX(strong_ctrl_login_cnt_per_month) AS strong_ctrl_login_cnt_per_month,
			MAX(normal_ctrl_login_cnt_per_month) AS normal_ctrl_login_cnt_per_month,
			MAX(CAST(natural_login_cnt_per_month AS DOUBLE) / user_cnt) AS natural_base_cnt,
			MAX(CAST(com_login_cnt_per_month AS DOUBLE) / user_cnt) AS com_base_cnt,
			MAX(CAST(free_login_cnt_per_month AS DOUBLE) / user_cnt) AS free_base_cnt,
			MAX(CAST(no_ctrl_login_cnt_per_month AS DOUBLE) / user_cnt) AS no_ctrl_base_cnt,
			MAX(CAST(strong_ctrl_login_cnt_per_month AS DOUBLE) / user_cnt) AS strong_ctrl_base_cnt,
			MAX(CAST(normal_ctrl_login_cnt_per_month AS DOUBLE) / user_cnt) AS normal_ctrl_base_cnt
		FROM
			app.app_yhzz_umc_unit_grid
		WHERE
			dt ='{part_dt}'
			AND grid_name_1st IS NOT NULL
			AND is_grid_valid = 1
		GROUP BY
			grid_name_1st
	)
	grid
ON
	new.grid_name = grid.grid_name_1st
left join
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