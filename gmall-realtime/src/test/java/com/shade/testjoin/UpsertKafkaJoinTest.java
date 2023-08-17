package com.shade.testjoin;

import com.shade.bean.Name;
import com.shade.bean.Sex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/20 19:49
 * @description:
 */
public class UpsertKafkaJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置他变了状态的过期时间
     //   tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

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

        //创建upsertkafka表
        tableEnv.executeSql("CREATE TABLE test ( " +
                "  id STRING, " +
                "  name STRING, " +
                "  sex STRING, " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = 'test', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")");

        tableEnv.executeSql("insert into test select " +
                "n.id,n.name,s.sex " +
                "from n " +
                "left join " +
                "s " +
                "on  " +
                "n.id=s.id");

        //{"id":"2","name":"bb","sex":null}
        //null
        //{"id":"2","name":"bb","sex":"xx"}
        //中间有null值 就是做了修改操作,先删除再添加
        env.execute();
    }
}
