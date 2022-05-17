package com.skydp.chaindata.realtime.app.dwd;

import com.skydp.chaindata.realtime.utils.ConfigUtil;
import com.skydp.chaindata.realtime.utils.SkyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CoinPriceApp {
    public static void main(String[] args) throws Exception {
        //Create the Flink flow processing execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Parallelism is synchronized with the Kafka partition
        //env.setParallelism(3);
        //checkpoint every 5 minutes
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://"+ ConfigUtil.getProperty("NAMENODE_HOST_CONFIG") + ":8020" + ConfigUtil.getProperty("CHECKPOINT_PATH_CONFIG"));
        //checkpoint has to be done in one minute or it's thrown out
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000);

        System.setProperty("HADOOP_USER_NAME","hdfs");

        //设置kafka消费主题和消费组
        String groupId = "dwd_coin_price";
        String topic = "coin_price";

        //获取kafkasource
        KafkaSource<String> kafkaSource = SkyKafkaUtil.getKafkaSource(topic, groupId);

        //flink连接kafka
        DataStreamSource<String> kafkaDS = env
                .fromSource(kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        /* 使用事务事件水位线
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(
                                Duration.ofMinutes(1)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return JSON.parseObject(element).getLong("timestamp");
                            }
                        }), */
                        "Kafka Source").setParallelism(3);

        kafkaDS.print();

        env.execute();

    }
}
