package com.yqy.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bahsk
 * @createTime 2022-02-13 0:49
 * @description
 * @program: flink-imooc
 */
public class BatchWCApp {
    public static void main(String[] args) throws Exception {
        //上下文
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataSource<String> source = executionEnvironment.readTextFile("data/wc.txt");

        //业务处理
        source.flatMap(new PKFlatMapFunction())
                .map(new PKMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }



}
class PKFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for(String word : words) {
            out.collect(word.toLowerCase().trim());
        }
    }
}

class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
    }
}
