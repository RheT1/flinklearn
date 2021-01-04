package com.yyx.hotitemsanalysis;

import com.yyx.hotitemsanalysis.bean.ItemViewCount;
import com.yyx.hotitemsanalysis.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Copyright(c) 2020-2030 YUAN All Rights Reserved
 * <p>
 * Project: bigdata
 * Package: com.yyx.hotitemsanalysis
 * Version: 1.0
 * <p>
 * Created by yyx on 2021/1/4
 */
public class HotItems1 {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2、读取数据，创建DataStream
        String path = HotItems1.class.getClassLoader().getResource("UserBehavior.csv").getPath();
        DataStreamSource<String> inputStream = env.readTextFile(path);
        // 3、转换为POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream
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

        // 4、分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤pv行为
                .keyBy(UserBehavior::getItemId) // 按商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5)) // 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        // 5、收集同一窗口的所有商品的count数据，排序输出top n
        DataStream<String> resultDataStream = (DataStream<String>) windowAggStream
                .keyBy(ItemViewCount::getWindowEnd) // 按照窗口分组
                .process(new TopNHotItems(5)); // 用自定义处理函数排序取前5

        resultDataStream.print();

        env.execute("hot items analysis");

    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }


        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long aLong, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {

            long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(aLong,windowEnd,count));
        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long,ItemViewCount,String> {
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }


        //定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String,方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==================================\n");
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n");

            //遍历列表，取出top n输出
            for (int i = 0; i < Math.min(topSize,itemViewCounts.size()); i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(itemViewCount.getItemId())
                        .append(" 热门度 = ").append(itemViewCount.getCount())
                        .append("\n");
            }

            resultBuilder.append("==================================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
