package com.yyx.networkflow_analysis;

import com.yyx.networkflow_analysis.beans.LogEvent;
import com.yyx.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Copyright(c) 2020-2030 YUAN All Rights Reserved
 * <p>
 * Project: bigdata
 * Package: com.yyx.networkflow_analysis
 * Version: 1.0
 * <p>
 * Created by yyx on 2021/1/3
 */
public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = HotPages.class.getResource("/apache.log");
        DataStreamSource<String> dataStreamSource = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<LogEvent> dataStream = dataStreamSource.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long time = sdf.parse(fields[3]).getTime();
            return new LogEvent(fields[0], fields[1], time, fields[5], fields[6]);
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(LogEvent logEvent) {
                        return logEvent.getTimestamp();
                    }
                });
        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String reg = "^((?!\\.(ico|png|js|ttf|gif|css|jpeg|jpg)$).)*$";
                    return Pattern.matches(reg, data.getUrl());
                })
                .keyBy(LogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new PageCountAgg(),new PageCountResult());

        // 收集同一窗口count数据，排序shuchu
        SingleOutputStreamOperator<String> resulteStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPage(3));

        resulteStream.print();

        env.execute("hot page top");

    }

    // 自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<LogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(LogEvent logEvent, Long aLong) {
            return aLong += 1;
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

    // 自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long,PageViewCount,String,TimeWindow>{
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(s,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    // 自定义的处理函数
    public static class TopNHotPage extends KeyedProcessFunction<Long,PageViewCount,String>{

        private Integer topSize;

        public TopNHotPage(Integer topSize) {
            this.topSize = topSize;
        }

        //定义列表状态，保存当前窗口内所有输出的PageViewCount
        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-view-count-list",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            pageViewCountListState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    if(o1.getCount() > o2.getCount())
                        return -1;
                    else if(o1.getCount() < o2.getCount())
                        return 1;
                    else
                        return 0;
                }
            });
            // 将排名信息格式化成String,方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==================================\n");
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n");

            //遍历列表，取出top n输出
            for (int i = 0; i < Math.min(topSize,pageViewCounts.size()); i++){
                PageViewCount pageViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 页面URL = ").append(pageViewCount.getUrl())
                        .append(" 浏览量 = ").append(pageViewCount.getCount())
                        .append("\n");
            }

            resultBuilder.append("==================================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
