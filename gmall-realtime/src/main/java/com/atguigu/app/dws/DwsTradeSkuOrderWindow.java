package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeSkuOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: shade
 * @date: 2022/7/29 16:24
 * @description:
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafka dws下单数据 转为json对象
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_trade_order_detail", "DwsTradeSkuOrderWindow"));
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception ignored) {
                }
            }
        });
 //       jsonDS.print();

        //TODO 按照id分组 状态编程剔除掉upsertkafka 中的修改数据 ,即取最大事件时间
        SingleOutputStreamOperator<JSONObject> processDS = jsonDS.keyBy(json -> json.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("lastState", JSONObject.class);

                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10L)).build());
                        lastState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastData = lastState.value();
                        if (lastData == null) {
                            lastState.update(value);
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                        } else {
                            String lasttime = lastData.getString("row_op_ts");
                            String curtime = value.getString("row_op_ts");
                            if (curtime.compareTo(lasttime) >= 0) {
                                lastState.update(value);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = lastState.value();
                        if (jsonObject != null) {
                            out.collect(jsonObject);
                        }
                        lastState.clear();
                    }
                });

        //TODO 添加时间戳 转为javabean
        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = processDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return DateFormatUtil.toTs(element.getString("create_time"),true);
                    }
                }));

        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = jsonWithWMDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                String sku_id = value.getString("sku_id");
                String sku_name = value.getString("sku_name");
                Double split_original_amout = value.getDouble("split_original_amout");
                Double split_total_amount = value.getDouble("split_total_amount");
                Double split_activity_amount = value.getDouble("split_activity_amount");
                if (split_activity_amount == null) {
                    split_activity_amount = 0.0;
                }
                Double split_coupon_amount = value.getDouble("split_coupon_amount");
                if (split_coupon_amount == null) {
                    split_coupon_amount = 0.0;
                }
                Set<String> set = new HashSet<>();
                set.add(value.getString("order_id"));
                return TradeSkuOrderBean.builder()
                        .skuId(sku_id)
                        .skuName(sku_name)
                        .orderIds(set)
                        .originalAmount(split_original_amout)
                        .orderAmount(split_total_amount)
                        .activityAmount(split_activity_amount)
                        .couponAmount(split_coupon_amount)
                        .build();
            }
        });

        //TODO 按照sku_id分组 开窗 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.getOrderIds().addAll(value2.getOrderIds());
                        value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                        value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                        value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean next = input.iterator().next();
                        next.setOrderCount((long) next.getOrderIds().size());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });
        reduceDS.print();

        //TODO  添加维度字段


        //TODO  写到clickhouse
        //TODO  启动
        env.execute();

    }
}
