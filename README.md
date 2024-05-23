### EMR Flink Hbase Simple Example
#### datagen to hbase
* EMR version：6.15.0 (flink-1.17.1 hbase 2.4.17)
* 编译
```shell
mvn clean package -Dscope.type=provided
# 可以直接用如下编译好的包
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-flink-hbase-1.0.jar
```
* 提交作业
```shell
# 创建hbase table
create 'hbase_datagen_table', 'cf'
```
```shell
# 注意替换为你自己的s3 bucket和hbase zk地址
checkpoints=s3://xxxx/flink/checkpoints/
hbase_zk="xxx.xxx.xxx.xxx:2181"
sudo flink run-application -t yarn-application \
-D state.backend=rocksdb \
-D state.checkpoint-storage=filesystem \
-D state.checkpoints.dir=${checkpoints} \
-D execution.checkpointing.interval=60000 \
-D state.checkpoints.num-retained=5 \
-D execution.checkpointing.mode=EXACTLY_ONCE \
-D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
-D state.backend.incremental=true \
-D execution.checkpointing.max-concurrent-checkpoints=1 \
-D rest.flamegraph.enabled=true \
-D jobmanager.memory.process.size=2048m \
-D taskmanager.memory.process.size=2048m \
-D taskmanager.numberOfTaskSlots=4 \
-D parallelism.default=4 \
-D yarn.application-attempts=1 \
-D yarn.application.name="datagen-sink-hbase" \
./emr-flink-hbase-1.0.jar \
--hbase_zk  ${hbase_zk}
```
![flink-ui](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202405231407892.png)
![hbase-scan](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202405231404755.png)
