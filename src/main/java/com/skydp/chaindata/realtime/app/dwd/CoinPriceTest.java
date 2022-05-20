package com.skydp.chaindata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skydp.chaindata.realtime.bean.CoinPrice;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class CoinPriceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.readTextFile("input/coin_price.txt");

        SingleOutputStreamOperator<CoinPrice> coinPriceStream= ds.map(line -> {
            CoinPrice coinPrice = JSON.parseObject(line, CoinPrice.class);
            String web_time = coinPrice.getWeb_time();
            String[] web_time_arr = web_time.split(" ");
            coinPrice.setDate(web_time_arr[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            coinPrice.setTs(sdf.parse(web_time).getTime());

            return coinPrice;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<CoinPrice>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<CoinPrice>() {
                    @Override
                    public long extractTimestamp(CoinPrice coinPrice, long l) {
                        return coinPrice.getTs();
                    }
                }));

        coinPriceStream.print();

        env.execute();
    }
}

