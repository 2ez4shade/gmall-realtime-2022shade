package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserRegisterBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/27 13:03
 * @description:
 */
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 读取dwd用户注册表
        String topic = "dwd_user_register";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(topic, "DwsUserUserRegisterWindow"));
        kafkaDS.print("kafka");
        //TODO 转为javabean ,并添加watermarks
        SingleOutputStreamOperator<UserRegisterBean> beanWithWKDS = kafkaDS.process(new ProcessFunction<String, UserRegisterBean>() {
            @Override
            public void processElement(String value, ProcessFunction<String, UserRegisterBean>.Context ctx, Collector<UserRegisterBean> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                out.collect(new UserRegisterBean("", "", 1L,
                        DateFormatUtil.toTs(jsonObject.getString("create_time"), true)));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));
        beanWithWKDS.print("bean");
        //TODO 开窗
        AllWindowedStream<UserRegisterBean, TimeWindow> windowDS = beanWithWKDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceDS = windowDS.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                UserRegisterBean registerBean = values.iterator().next();
                registerBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                registerBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                registerBean.setTs(System.currentTimeMillis());
                out.collect(registerBean);
            }
        });

        //TODO 写出到clickhouse
        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_user_user_register_window values(?,?,?,?)"));

        //TODO 启动
        env.execute();
    }
}
