package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/27 20:31
 * @description:
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafka加购topic,转为json对象
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_trade_cart_add", "DwsTradeCartAddUuWindow"));
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //TODO 添加watermark
        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String create_time = element.getString("create_time");
                        String operate_time = element.getString("operate_time");
                        if (operate_time == null) {
                            return DateFormatUtil.toTs(create_time, true);
                        } else {
                            return DateFormatUtil.toTs(operate_time, true);
                        }
                    }
                }));

        //TODO keyby
        KeyedStream<JSONObject, String> keybyDS = jsonWithWMDS.keyBy(json -> json.getString("user_id"));

        //TODO 状态编程,并转为javabean
        SingleOutputStreamOperator<CartAddUuBean> processDS = keybyDS.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
            ValueState<String> lastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastState", String.class);

                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {
                String lastdt = lastState.value();
                String curdt = "";
                String create_time = value.getString("create_time");
                String operate_time = value.getString("operate_time");
                if (operate_time == null) {
                    curdt = create_time.split(" ")[0];
                } else {
                    curdt = operate_time.split(" ")[0];
                }
                if (lastdt == null || !lastdt.equals(curdt)) {
                    lastState.update(curdt);
                    out.collect(new CartAddUuBean("", "", 1L, 0L));
                }

            }
        });
        //TODO 聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        //TODO 写到clickhouse
        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //TODO 启动
        env.execute();
    }
}
