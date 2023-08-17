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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/20 19:49
 * @description:
 */
public class SQlAPIJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置他变了状态的过期时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Name> nameDS = env.socketTextStream("hadoop102", 8888).map(new MapFunction<String, Name>() {
            @Override
            public Name map(String value) throws Exception {
                String[] split = value.split(",");
                return new Name(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        SingleOutputStreamOperator<Sex> sexDS = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, Sex>() {
            @Override
            public Sex map(String value) throws Exception {
                String[] split = value.split(",");
                return new Sex(split[0], split[1], Long.parseLong(split[2]));
            }
        });
        //流转表
        tableEnv.createTemporaryView("n",nameDS);
        tableEnv.createTemporaryView("s", sexDS);

        //join 2个表使用的是oncreateandwrite 数据会在创建和修改后重新定时
   //     tableEnv.sqlQuery("select * from n join s on n.id=s.id").execute().print();

        //left/right 主表使用的是onreadandwrite 数据会在创建和修改以及读后重新定时 从表使用 oncreateandwrite
        //     tableEnv.sqlQuery("select * from n left join s on n.id=s.id").execute().print();

        //full 主表从表都使用的是onreadandwrite 数据会在创建和修改以及读后重新定时
        tableEnv.sqlQuery("select * from n full join s on n.id=s.id").execute().print();

        env.execute();
    }
}
