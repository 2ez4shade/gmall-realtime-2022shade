package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/27 11:15
 * @description:
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafkapage数据 转为json 过滤出用户登录的数据
        String topic = "dwd_traffic_page_log";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(topic, "DwsUserUserLoginWindow2"));
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastpageid = jsonObject.getJSONObject("page").getString("last_page_id");
                if (uid != null && (lastpageid == null || "login ".equals(lastpageid))) {
                    out.collect(jsonObject);
                }
            }
        });

        //TODO 添加watermark
        SingleOutputStreamOperator<JSONObject> jsonWithWKDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //TODO 按照uid分组
        KeyedStream<JSONObject, String> keybyDS = jsonWithWKDS.keyBy(josn -> josn.getJSONObject("common").getString("uid"));

        //TODO 状态编程,判断是否是活跃用户和回流用户,转为javabean写出
        SingleOutputStreamOperator<UserLoginBean> processDS = keybyDS.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valueState", String.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                String lastdt = valueState.value();
                Long ts = value.getLong("ts");
                String curdt = DateFormatUtil.toDate(ts);

                // 回流用户数
                long backCt = 0L;
                // 独立用户数
                long uuCt = 0L;
                if (lastdt == null) {
                    uuCt = 1L;
                    valueState.update(curdt);
                } else if (!lastdt.equals(curdt)) {
                    uuCt = 1L;
                    if ((ts - DateFormatUtil.toTs(lastdt, false)) /( 24 * 3600 * 1000) > 7) {
                        backCt = 1L;
                    }
                    valueState.update(curdt);
                }
                if (uuCt == 1L) {
                    out.collect(new UserLoginBean("", "", backCt, uuCt, ts));
                }
            }
        });

        //TODO 开窗 聚合
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean loginBean = values.iterator().next();
                loginBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                loginBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                out.collect(loginBean);
            }
        });

        //TODO 写出到clickhouse
        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        //TODO 启动
        env.execute();
    }
}
