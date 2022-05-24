package com.skydp.chaindata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.skydp.chaindata.realtime.bean.FloorPrice;
import com.skydp.chaindata.realtime.utils.ConfigUtil;
import com.skydp.chaindata.realtime.utils.SkyClickhouseUtil;
import com.skydp.chaindata.realtime.utils.SkyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class FloorPriceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Parallelism is synchronized with the Kafka partition
        //env.setParallelism(3);
        //checkpoint every 5 minutes
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://" + ConfigUtil.getProperty("NAMENODE_HOST_CONFIG") + ":8020" + ConfigUtil.getProperty("CHECKPOINT_PATH_CONFIG"));
        //checkpoint has to be done in one minute or it's thrown out
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000);

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        //设置kafka消费主题和消费组
        String groupId = "dwd_floor_price";
        String topic = "opensea_floor_price";

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

        SingleOutputStreamOperator<FloorPrice> result = kafkaDS.process(new ProcessFunction<String, FloorPrice>() {
            @Override
            public void processElement(String line, Context context, Collector<FloorPrice> collector) throws Exception {
                try {
                    FloorPrice floorPrice = JSON.parseObject(line, FloorPrice.class);
                    String time = floorPrice.getData_collection_time();
                    String[] time_arr = time.split(" ");
                    floorPrice.setDate(time_arr[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    floorPrice.setTs(sdf.parse(time).getTime());

                    collector.collect(floorPrice);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<FloorPrice>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<FloorPrice>() {
                    @Override
                    public long extractTimestamp(FloorPrice floorPrice, long l) {
                        return floorPrice.getTs();
                    }
                }));

        String insertSql = "insert into floor_price values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        result.addSink(SkyClickhouseUtil.getJdbcSink(insertSql));

        env.execute("Floor Price");
    }
}
