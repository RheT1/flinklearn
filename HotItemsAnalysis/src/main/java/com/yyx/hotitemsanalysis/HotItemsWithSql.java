package com.yyx.hotitemsanalysis;

import com.yyx.hotitemsanalysis.bean.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Copyright(c) 2020-2030 YUAN All Rights Reserved
 * <p>
 * Project: bigdata
 * Package: com.yyx.hotitemsanalysis
 * Version: 1.0
 * <p>
 * Created by yyx on 2021/1/2
 */
public class HotItemsWithSql {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建tableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 读取数据，创建DataStream

        DataStreamSource<String> inputStream = env.readTextFile("D:\\Develop\\bigdata\\UserBehaviourAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] splits = line.split(",");
                    return new UserBehavior(Long.parseLong(splits[0]), Long.parseLong(splits[1]), Integer.parseInt(splits[2]), splits[3], Long.parseLong(splits[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });



        tableEnv.createTemporaryView("data_table",dataStream,"itemId, behavior, timestamp.rowtime as ts");

        // sql实现
        Table resultSqlTable = tableEnv.sqlQuery("select * from" +
                "( select *,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num " +
                "   from (" +
                "       select itemId,count(itemId) as cnt, HOP_END(ts,interval '5' minute,interval '1' hour) as windowEnd" +
                "       from data_table" +
                "       where behavior = 'pv'" +
                "       group by itemId,HOP(ts, interval '5' minute, interval '1' hour)" +
                "       )" +
                ")" +
                "where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql");

    }
}
