#bin/sh


BIZ_DATE=`date -d "-1 days" +%Y%m%d`
BIZ_DATE_5=`date -d "${BIZ_DATE} -4 days" +%Y%m%d`
BIZ_DATE_6=`date -d "${BIZ_DATE} -5 days" +%Y%m%d`
BIZ_DATE_46=`date -d "${BIZ_DATE} -45 days" +%Y%m%d`
DEL_DATE=`date -d "-3 days" +%Y%m%d`

DATA_PATH=/nfs/project/chuchu/MTL_deepfm/data

mkdir ${DATA_PATH}/${BIZ_DATE}

sql="
set mapreduce.job.queuename=root.kuaicheshiyebu-houshichangyewuxian.amdaalg;

insert overwrite local directory '${DATA_PATH}/tmp'
row format delimited
fields terminated by ','

select
   A.*
   ,B.label as cvr_label
from
( 
    select
        *
    from 
        am_bi.ads_algo_growth_mta_prob_mta_ltv_label_dd 
    where 
        dt between '${BIZ_DATE_46}' and '${BIZ_DATE_6}'
        and is_newer = 0
) A
join
(
    select
        distinct dt
        ,passport_uid
        ,label
    from
       am_bi.ads_algo_growth_mta_prob_mta_order_pred_label_dd
    where
        dt between '${BIZ_DATE_46}' and '${BIZ_DATE_6}'
        and is_newer = 0
) B on A.passport_uid = B.passport_uid and A.dt = B.dt
"

echo "${sql}"
hive -e "${sql}"

cat ${DATA_PATH}/header ${DATA_PATH}/tmp/0* >${DATA_PATH}/${BIZ_DATE}/raw_train_data.csv
head ${DATA_PATH}/${BIZ_DATE}/raw_train_data.csv
rm ${DATA_PATH}/tmp/0*  ${DATA_PATH}/tmp/.0*

sql="
set mapreduce.job.queuename=root.kuaicheshiyebu-houshichangyewuxian.amdaalg;

insert overwrite local directory '${DATA_PATH}/tmp'
row format delimited
fields terminated by ','

select
   A.*
   ,B.label as cvr_label
from
( 
    select
        *
    from 
        am_bi.ads_algo_growth_mta_prob_mta_ltv_label_dd 
    where 
        dt between '${BIZ_DATE_5}' and '${BIZ_DATE}'
        and is_newer = 0
) A
join
(
    select
        distinct dt
        ,passport_uid
        ,label
    from
       am_bi.ads_algo_growth_mta_prob_mta_order_pred_label_dd
    where
        dt between '${BIZ_DATE_5}' and '${BIZ_DATE}'
        and is_newer = 0
) B on A.passport_uid = B.passport_uid and A.dt = B.dt

"

echo "${sql}"
hive -e "${sql}"

cat ${DATA_PATH}/header ${DATA_PATH}/tmp/0* >${DATA_PATH}/${BIZ_DATE}/raw_test_data.csv
head ${DATA_PATH}/${BIZ_DATE}/raw_test_data.csv
rm ${DATA_PATH}/tmp/0*  ${DATA_PATH}/tmp/.0*
 

#LifeCycle

rm -rf ${DATA_PATH}/${DEL_DATE}
