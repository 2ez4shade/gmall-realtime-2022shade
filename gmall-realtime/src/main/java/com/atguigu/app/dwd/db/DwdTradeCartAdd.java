package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import com.atguigu.utils.MySqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/22 19:21
 * @description:
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 获取kafka topic_db中数据建json表
        tableEnv.executeSql(MyKafkaUtils.getTopicdbDDL("DwdTradeCartAdd"));

//        tableEnv.sqlQuery("select * from topic_db").execute().print();

        //TODO 查询过滤出 加购的数据
        Table cartinfo = tableEnv.sqlQuery("select " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['cart_price'] cart_price, " +
                "if(`type`='insert',`data`['sku_num'],cast((cast(`data`['sku_num'] as int)-cast(`old`['sku_num'] as int)) as string)) sku_num, " +
                "`data`['sku_name'] sku_name, " +
                "`data`['is_checked'] is_checked, " +
                "`data`['create_time'] create_time, " +
                "`data`['operate_time'] operate_time, " +
                "`data`['is_ordered'] is_ordered, " +
                "`data`['order_time'] order_time, " +
                "`data`['source_type'] source_type, " +
                "`data`['source_id'] source_id, " +
                "`pt` " +
                "from topic_db " +
                "where " +
                "`database`='gmall' and `table`='cart_info' " +
                "and (`type`='insert' or (`type`='update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int)-cast(`old`['sku_num'] as int)>0))");
        tableEnv.createTemporaryView("cartinfo",cartinfo);

   //     cartinfo.execute().print();
        //TODO 获取mysql中的字典表,做字段关联 lookupjoin
        tableEnv.executeSql(MySqlUtils.getBaseDic());
     //   tableEnv.sqlQuery("select * from base_dic").execute().print();

        Table resultable = tableEnv.sqlQuery("select " +
                "c.`id`,  " +
                "c.`user_id`,  " +
                "c.`sku_id`,  " +
                "c.`cart_price`,  " +
                "c.`sku_num`,  " +
                "c.`sku_name`,  " +
                "c.`is_checked`,  " +
                "c.`create_time`,  " +
                "c.`operate_time`,  " +
                "c.`is_ordered`,  " +
                "c.`order_time`,  " +
                "c.`source_type`, " +
                "b.`dic_name` source_name,  " +
                "c.`source_id`  " +
                "from cartinfo c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.`pt` b " +
                "on c.source_type=b.dic_code");
        //resultable.execute().print();
        tableEnv.createTemporaryView("resultable",resultable);

        //TODO 获取kafka对应写出表
        tableEnv.executeSql("create table dwd_trade_cart_add( " +
                "    `id` string, " +
                "    `user_id` string, " +
                "    `sku_id` string, " +
                "    `cart_price` string, " +
                "    `sku_num` string, " +
                "    `sku_name` string, " +
                "    `is_checked` string, " +
                "    `create_time` string, " +
                "    `operate_time` string, " +
                "    `is_ordered` string, " +
                "    `order_time` string, " +
                "    `source_type` string, " +
                "    `source_name` string, " +
                "    `source_id` string " +
                ")" + MyKafkaUtils.getInsertKafka("dwd_trade_cart_add"));

        //TODO 查询写出
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from resultable");


    }
}
