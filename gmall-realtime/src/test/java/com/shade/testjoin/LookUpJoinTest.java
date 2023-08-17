package com.shade.testjoin;

import com.shade.bean.Name;
import com.shade.bean.Sex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: shade
 * @date: 2022/7/20 19:49
 * @description:
 */
public class LookUpJoinTest {
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
        tableEnv.createTemporaryView("n",nameDS,$("id"),$("name"),$("ts"),$("pt").proctime());

        tableEnv.executeSql("CREATE TEMPORARY TABLE base_sale_attr ( " +
                "  id bigint, " +
                "  name STRING, " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall', " +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',"+
                "  'table-name' = 'base_sale_attr', " +
                "  'username' = 'root', " +
                //这2个字段会将查到的字段缓存,不会缓存没查到字段,应该用了一个map保存起来了,查之前看看keyset中有没有,有就不查
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'password' = '123456' " +
                ")");

        tableEnv.sqlQuery("select  " +
                "*  " +
                "from  " +
                "n  " +
                "join  " +
                "base_sale_attr  FOR SYSTEM_TIME AS OF n.pt " +
                "on n.name=base_sale_attr.name").execute().print();

    }
}
