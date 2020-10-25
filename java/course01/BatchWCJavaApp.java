package com.flink.java.course01;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception{
        String input = "file:///E:/flink_quickstart_java/local/kv.txt";
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读数
        DataSource<String> text = env.readTextFile(input);
        text.print();
        //transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(" ");
                for(String token:tokens){
                    if(token.length()>0){
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();



    }
}
