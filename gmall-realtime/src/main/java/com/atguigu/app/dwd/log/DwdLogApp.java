package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: shade
 * @date: 2022/7/19 16:44
 * @description:
 */
public class DwdLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //测试不使用 麻烦
//        // 状态后端设置
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
        //TODO 从kafka中消费topiclog数据
        String topic = "topic_log";
        String groupId = "DwdLogApp";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(topic, groupId));


        //TODO 转为json并过滤
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        //打印错误的


        //TODO 按照midkeyby
        KeyedStream<JSONObject, String> keyByDS = jsonDS.keyBy((KeySelector<JSONObject, String>) value -> value.getJSONObject("common").getString("mid"));

        //TODO 状态编程 对isnew字段处理
        SingleOutputStreamOperator<JSONObject> mapDS = keyByDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valueState", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isnew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isnew)) {
                    if (valueState.value() == null) {
                        //将当前的日期给到状态
                        String date = DateFormatUtil.toDate(value.getLong("ts"));
                        valueState.update(date);
                    } else if (!valueState.value().equals(DateFormatUtil.toDate(value.getLong("ts")))) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (valueState.value() == null) {
                    valueState.update(DateFormatUtil.toDate(value.getLong("ts") - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });

        //TODO 分流
        OutputTag<String> errorTag = new OutputTag<String>("errotTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };

        SingleOutputStreamOperator<String> processDS = mapDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //先判断是否为error 是写出并remove
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    ctx.output(errorTag, err.toJSONString());
                }
                value.remove("err");
                //判断是否是start 是写出并remove,不是 说明一定是page
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //后面用来补充字段
                    JSONObject common = value.getJSONObject("common");
                    Long ts = value.getLong("ts");
                    //判断是否有actions 遍历写出
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }
                    //判断是否有displays 遍历写出
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //remove掉除page,common,ts之外的字段
                    value.remove("actions");
                    value.remove("displays");
                    //写出page
                    out.collect(value.toJSONString());
                }

            }
        });

        //TODO 写出
        DataStream<String> errorDS = processDS.getSideOutput(errorTag);
        DataStream<String> startDS = processDS.getSideOutput(startTag);
        DataStream<String> actionDS = processDS.getSideOutput(actionTag);
        DataStream<String> displayDS = processDS.getSideOutput(displayTag);

        // 7.2 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";


        jsonDS.getSideOutput(dirtyTag).print("错误的Json");

        errorDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(error_topic));
        startDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(start_topic));
        actionDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(action_topic));
        displayDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(display_topic));
        processDS.addSink(MyKafkaUtils.getFlinkKafkaProducer(page_topic));

        env.execute();
    }
}
