file_path="/home/yuchuchu/machine/machine_algo/用户管理中心/2流量生成模块/log/"
start_time='2022-07-01'
echo start_time$file_path
echo $start_time

# 逐次执行

for ((i=1; i<=10; i++))
do

# 运行时间
run_day=`date -d "$start_time $i days" "+%Y-%m-%d"`
echo $run_day
log_file=$file_path$i'.log'
echo $log_file

#执行并记录日志 dev_start.sh
# sh online_start.sh $run_day 'low_test'  "('A1','A2')" >>$log_file 2>&1 
# sh dev_start.sh $run_day 'low_test'  "('A1','A2')" >>$log_file 2>&1 
sh dev_start.sh $run_day 'low_test'  "('gy_new')" >>$log_file 2>&1 

if [ $? -ne 0 ]; then
    echo "failed"
else
    echo "succeed"
fi

done