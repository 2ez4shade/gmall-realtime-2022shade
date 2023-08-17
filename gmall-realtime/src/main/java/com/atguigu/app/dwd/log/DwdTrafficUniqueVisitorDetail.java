package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/20 10:44
 * @description:
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        
        //TODO 从Kafka读取数据
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "UniqueVisitorDetail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(pageTopic, groupId));

        //TODO 转为JSON对象并过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String lastpage = jsonObject.getJSONObject("page").getString("last_page_id");
                    System.out.println(jsonObject);
                    if (lastpage==null){
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println(value + "---不是json格式");
                }
            }
        });

        //TODO keyby
        KeyedStream<JSONObject, String> keyByDS = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 键控状态编程
        SingleOutputStreamOperator<JSONObject> filterDS = keyByDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //配置ttl
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastDtState", String.class);
                //ttl配置信息
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(stateTtlConfig);

                lastDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastDt = lastDtState.value();
                String pageDt = DateFormatUtil.toDate(value.getLong("ts"));
                if (lastDt == null || !lastDt.equals(pageDt)) {
                    lastDtState.update(pageDt);
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 写到kafka对应topic
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer(targetTopic));
        
        //TODO 启动
        env.execute("DwdTrafficUniqueVisitorDetail");


    }
}
