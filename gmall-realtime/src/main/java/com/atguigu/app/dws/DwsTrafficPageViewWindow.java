package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/26 17:19
 * @description:
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafka page 
        String topic = "dwd_traffic_page_log";
        String groupId = "DwsTrafficPageViewWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(topic, groupId));

        //TODO 过滤并转为josn对象 并添加watermark
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
      //          System.out.println(page_id);
                if ("home".equals(page_id) || "good_detail".equals(page_id)) {
                    out.collect(jsonObject);
                }

            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));
   //     jsonDS.print("json:");

        //TODO keyby
        KeyedStream<JSONObject, String> keyByDS = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //TODO 状态编程 2个日期,输出javabean
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processDS = keyByDS.process(new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homestate;
            ValueState<String> detailstate;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeStateDescriptor = new ValueStateDescriptor<>("homestate", String.class);
                ValueStateDescriptor<String> detailStateDescriptor = new ValueStateDescriptor<>("detailstate", String.class);
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();

                homeStateDescriptor.enableTimeToLive(stateTtlConfig);
                detailStateDescriptor.enableTimeToLive(stateTtlConfig);
                homestate = getRuntimeContext().getState(homeStateDescriptor);
                detailstate = getRuntimeContext().getState(detailStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String homedt = homestate.value();
                String detaildt = detailstate.value();
                Long ts = value.getLong("ts");
                String curdt = DateFormatUtil.toDate(ts);
                String pageid = value.getJSONObject("page").getString("page_id");
      //          System.out.println(curdt);
       //         System.out.println(pageid);
                long homeUvCt = 0L;
                long goodDetailUvCt = 0L;
                if ("home".equals(pageid) && (homedt == null || !homedt.equals(curdt))) {
                    homeUvCt = 1L;
                    homestate.update(curdt);
                } else if ("good_detail".equals(pageid) && (detaildt == null || !detaildt.equals(curdt))) {
                    goodDetailUvCt = 1L;
                    detailstate.update(curdt);
                }
                if (homeUvCt == 1L || goodDetailUvCt == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, 0L));
                }
            }
        });



        //TODO 开窗 聚合
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {

            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean bean = values.iterator().next();
                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                bean.setTs(System.currentTimeMillis());
                out.collect(bean);
            }
        });

        //TODO 写到clickhouse
        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));
        //TODO 启动
        env.execute();
        //TODO 
    }
}
