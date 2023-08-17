package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: shade
 * @date: 2022/7/20 15:39
 * @description:
 */
//数据流 web/app->ng->日志服务器文件->flume->kafka(zk)->
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //状态后端设置
        //        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(
        //                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //        );
        //        env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        //        ));
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO kafka读取
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "TrafficUserJumpDetail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(pageTopic, groupId));
        //TODO 转为Json 并过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(value + "--------不是JSON格式");
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO keyby
        KeyedStream<JSONObject, String> midDS = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 创建匹配器
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).times(2).consecutive().within(Time.seconds(10));

        //TODO 将匹配器作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(midDS, pattern);

        //TODO select+测输出
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag,
                (PatternTimeoutFunction<JSONObject, String>) (pattern1, timeoutTimestamp) -> {
                    return pattern1.get("start").get(0).toJSONString();
                },
                (PatternSelectFunction<JSONObject, String>) pattern12 -> {
                    return pattern12.get("start").get(0).toJSONString();
                });

        //TODO  union主流和测输出流
        DataStream<String> timeoutDS = selectDS.getSideOutput(outputTag);

        selectDS.print("主流----:");
        timeoutDS.print("timeout----:");

        DataStream<String> unionDS = selectDS.union(timeoutDS);

        //TODO 写到kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(targetTopic));

        //TODO 启动
        env.execute();


    }
}
