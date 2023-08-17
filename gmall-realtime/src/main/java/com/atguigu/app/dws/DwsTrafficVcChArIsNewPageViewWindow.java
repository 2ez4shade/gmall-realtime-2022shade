package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/26 15:16
 * @description:
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafka uv 传为javabean
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> uvkafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(uvTopic, "DwsTrafficVcChArIsNewPageViewWindow"));
        SingleOutputStreamOperator<TrafficPageViewBean> uvDS = uvkafkaDS.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean(
                        "", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"),
                        1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts")
                );
            }
        });

        //TODO 读取kakfa uj 转为javabean
        String ujTopic = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> ujkafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(ujTopic, "DwsTrafficVcChArIsNewPageViewWindow"));
        SingleOutputStreamOperator<TrafficPageViewBean> ujDS = ujkafkaDS.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean(
                        "", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"),
                        0L, 0L, 0L, 0L, 1L, jsonObject.getLong("ts")
                );
            }
        });

        //TODO 读取kafka page 转为javabean
        String pageTopic = "dwd_traffic_page_log";
        DataStreamSource<String> pagekafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(pageTopic, "DwsTrafficVcChArIsNewPageViewWindow"));
        SingleOutputStreamOperator<TrafficPageViewBean> pageDS = pagekafkaDS.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                return new TrafficPageViewBean(
                        "", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"),
                        0L, page.get("last_page_id") == null ? 1L : 0L, 1L, page.getLong("during_time"), 0L, jsonObject.getLong("ts")
                );
            }
        });

   //     pageDS.print();

        //TODO 3流union 并生成watermark
        DataStream<TrafficPageViewBean> unionDS = uvDS.union(ujDS).union(pageDS);
        SingleOutputStreamOperator<TrafficPageViewBean> wkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO keyby分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keybyDS = wkDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return Tuple4.of(value.getIsNew(), value.getAr(), value.getCh(), value.getVc());
            }
        });

        //TODO 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = keybyDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {

                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        System.out.println( value1.getDurSum());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        return value1;
                    }

                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {

                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(pageViewBean);
                    }

                });

        //TODO 写到hbase
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 启动
        env.execute();
    }
}
