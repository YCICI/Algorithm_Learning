pip list

spark-submit \
    --master yarn-cluster \
    --conf spark.pyspark.python=python3 \
    --conf spark.yarn.maxAppAttempts=2 \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_jypt:latest \
    --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_jypt:latest  \
    --conf spark.sql.execution.arrow.enabled=true \
    --conf spark.yarn.executor.memoryOverhead=4g \
    --conf spark.default.parallelism=7500 \
    --conf spark.sql.hive.mergeFiles=true \
    --conf spark.sql.hive.merge.smallfile.boundary.size=128000000 \
    --conf spark.sql.hive.merge.smallfile.target.size=256000000 \
    --conf spark.executor.heartbeatInterval=100s \
    --conf spark.shuffle.io.maxRetries=10 \
    --conf spark.sql.orc.filterPushdown=true \
    --conf spark.sql.orc.impl=native \
    --conf spark.sql.orc.enableVectorizedReader=true \
    --conf spark.sql.hive.convertMetastoreOrc=true \
    --executor-memory 8g \
    --executor-cores 2 \
    --driver-memory 12g \
    --num-executors 450 \
    --py-files src/config.py \
    --files data/get_base_data.sql,data/get_newuser_base_data.sql,data/get_lastday_data.sql \
    --conf spark.sql.shuffle.partitions=800 \
    dev_umc_algo_pin_interim.py $*