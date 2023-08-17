package com.shade.testjoin;

import com.shade.bean.Name;
import com.shade.bean.Sex;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/20 19:49
 * @description:
 */
public class JoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Name> nameDS = env.socketTextStream("hadoop102", 8888).map(new MapFunction<String, Name>() {
            @Override
            public Name map(String value) throws Exception {
                String[] split = value.split(",");
                return new Name(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Name>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Name>) (element, recordTimestamp) -> element.getTs()*1000L));


        SingleOutputStreamOperator<Sex> sexDS = env.socketTextStream("hadoop102", 9999).map((MapFunction<String, Sex>) value -> {
            String[] split = value.split(",");
            return new Sex(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Sex>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Sex>) (element, recordTimestamp) -> element.getTs()*1000L));


        nameDS.keyBy(Name::getId)
                .intervalJoin(sexDS.keyBy(Sex::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Name, Sex, Tuple2<Name, Sex>>() {
                    @Override
                    public void processElement(Name left, Sex right, ProcessJoinFunction<Name, Sex, Tuple2<Name, Sex>>.Context ctx, Collector<Tuple2<Name, Sex>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                }).print(">>>>>>>>>>>");

        env.execute();
    }
}
