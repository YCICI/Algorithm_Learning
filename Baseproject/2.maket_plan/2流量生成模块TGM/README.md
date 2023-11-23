## 流量生成模块说明文档


### 1、背景说明

给定业务模式，在成本、资源约束条件下，通过运筹优化和业务准则结合，最优化规划各个渠道的月周日流量任务，实现业务目标的最大化(跃迁数、用户数、GMV等)。


### 2、主要模块结构  


a.运行环境和实现说明


* 运行环境：bdp wise_mart_jypt:latest
* 实现说明：采用spark udf的方式实现pin粒度的分发


b.代码结构介绍

* data/: 存放测试数据/获取数据的sql代码
* log/: 存放日志文件
* src/: 源代码，包含月周规划month_week_plan，日规划day_plan
* test/: 测试样例

c.代码功能说明

src/month_week_plan中的代码文件功能说明

* src/month_week_plan/cycle_online_start.sh: 按日期逐天补数  
* src/month_week_plan/dev_algo_pin_interim.py: 生成月周规划（开发）
* src/month_week_plan/dev_start.sh:  spark submit 执行月周规划py（开发）文件
* src/month_week_plan/online_udf_simulation.py: 生成月周规划（线上执行）
* src/month_week_plan/online_start.sh:  执行月周规划py（线上执行）文件
* src/month_week_plan/get_base_data.sql:  获取基础数据
* src/month_week_plan/get_lastday_data.sql:  获取过去一天的月周规划 
* src/month_week_plan/get_newuser_base_data.sql:  获取广义新基础数据

* src/month_week_plan/solve_by_udf.py:  月周规划测试样例


src/day_plan中的代码文件功能说明

* src/day_plan/dayplan_test.py  
* src/day_plan/dev_dayplan_result.py  
* src/day_plan/dev_dayplan.sh
* src/day_plan/tb_day_plan.ipynb

d.代码模块说明

### 4、使用说明

EA机器执行
```shell
# sh dev_start.sh 日期 方案档位low/medium/high/max  用户类型 "('gy_new')" /"('A1','A2')" /"('B1')" /"('C')"
sh dev_start.sh '2022-07-01' 'low_test'  "('gy_new')" 
```
BDP调度参数说明
```shell
# 日期；方案档位；用户类型
${fmt(add(NTIME(),-1,'day'),'yyyy-MM-dd')};low;('\'\A1\'','\'\A2\'')
```



参考文档
- https://cf.jd.com/pages/viewpage.action?pageId=862434433
- 模块输出字段详见 https://joyspace.jd.com/sheets/TzBJvql5U0u3IbuJJJzX
