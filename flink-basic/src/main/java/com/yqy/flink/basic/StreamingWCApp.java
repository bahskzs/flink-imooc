package com.yqy.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @author bahsk
 * @createTime 2022-02-13 0:27
 * @description
 * @program: flink-imooc
 */
public class StreamingWCApp {

    public static void main(String[] args) throws Exception {

        //创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //对接数据源
        DataStreamSource<String> streamSource = env.socketTextStream("47.114.63.55", 9870);

        //业务处理

        //每行根据逗号拆分
        streamSource.flatMap(new StreamingWCFlatMapFunction()).
                filter((FilterFunction<String>) value -> StringUtils.isNotEmpty(value)).
                map((MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, 1)).
                keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0).sum(1)
                .print();


        env.execute("StreamingWCApp");


    }

    private static class StreamingWCFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(word.toLowerCase().trim());
            }
        }
    }
}
