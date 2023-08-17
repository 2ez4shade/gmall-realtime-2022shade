package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import com.atguigu.utils.MySqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/23 11:25
 * @description:
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的保存时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

        //TODO kafka读取topicdb
        tableEnv.executeSql(MyKafkaUtils.getTopicdbDDL("DwdTradeOrderPreProcess"));

        //TODO 查询出订单表
        Table order_info = tableEnv.sqlQuery("select " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['operate_time'] operate_time, " +
                "`data`['province_id'] province_id, " +
                "`data`['order_status'] order_status, " +
                "`type`, " +
                "`old`, " +
                "`ts` oi_ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and (`type`='insert' or `type`='update')");
        tableEnv.createTemporaryView("order_info",order_info);
  //      order_info.execute().print();

        //TODO 查询出订单明细表
        Table order_detail = tableEnv.sqlQuery("select " +
                "`data`['id'] id, " +
                "`data`['order_id'] order_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['sku_name'] sku_name, " +
                "`data`['sku_num'] sku_num, " +
                "cast(cast(`data`['order_price'] as decimal(16,2)) * cast(`data`['sku_num'] as decimal(16,2)) as string) split_original_amout, " +
                "`data`['create_time'] create_time, " +
                "`data`['source_type'] source_type, " +
                "`data`['source_id'] source_id, " +
                "`data`['split_total_amount'] split_total_amount, " +
                "`data`['split_activity_amount'] split_activity_amount, " +
                "`data`['split_coupon_amount'] split_coupon_amount, " +
                "`ts` od_ts, " +
                " proctime() `pt` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail",order_detail);
 //       order_detail.execute().print();

        //TODO 查询出活动表
        Table order_detail_activity = tableEnv.sqlQuery("select " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['activity_id'] activity_id, " +
                "`data`['activity_rule_id'] activity_rule_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",order_detail_activity);


        //TODO 查询出购物券表
        Table order_detail_coupon = tableEnv.sqlQuery("select " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['coupon_id'] coupon_id, " +
                "`data`['coupon_use_id'] coupon_use_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_coupon",order_detail_coupon);


        //TODO lookup mysql base_dic
        tableEnv.executeSql(MySqlUtils.getBaseDic());

        //TODO 5表关联
        Table restable = tableEnv.sqlQuery("select " +
                "od.`id`, " +
                "od.`order_id`, " +
                "oi.`user_id`, " +
                "oi.`order_status`, " +
                "od.`sku_id`, " +
                "od.`sku_name`, " +
                "oi.`province_id`, " +
                "oa.`activity_id`, " +
                "oa.`activity_rule_id`, " +
                "oc.`coupon_id`, " +
                "date_format( od.`create_time`,'yyyy-MM-dd') date_id, " +
                "od.`create_time`, " +
                "date_format( oi.`operate_time`,'yyyy-MM-dd') operate_date_id, " +
                "oi.`operate_time`, " +
                "od.`source_type`, " +
                "od.`source_id`, " +
                "dic.`dic_name` source_type_name, " +
                "od.`sku_num`, " +
                "od.`split_original_amout`, " +
                "od.`split_total_amount`, " +
                "od.`split_activity_amount`, " +
                "od.`split_coupon_amount`, " +
                "oi.`type`, " +
                "oi.`old`, " +
                "od.`od_ts`, " +
                "oi.`oi_ts`, " +
                "current_row_timestamp() row_op_ts " +
                "from order_detail od  " +
                "join order_info oi " +
                "on od.order_id=oi.id  " +
                "left join order_detail_activity oa " +
                "on od.id=oa.order_detail_id " +
                "left join order_detail_coupon oc " +
                "on od.id=oc.order_detail_id " +
                "join base_dic for system_time as of od.pt dic " +
                "on od.source_type=dic.dic_code");
        tableEnv.createTemporaryView("restable",restable);

        //TODO 建立upset_kafka表 写出
        tableEnv.executeSql("create table dtopp( " +
                "`id` string, " +
                "`order_id` string, " +
                "`user_id` string, " +
                "`order_status` string, " +
                "`sku_id` string, " +
                "`sku_name` string, " +
                "`province_id` string, " +
                "`activity_id` string, " +
                "`activity_rule_id` string, " +
                "`coupon_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`operate_date_id` string, " +
                "`operate_time` string, " +
                "`source_type` string, " +
                "`source_id` string, " +
                "`source_type_name` string, " +
                "`sku_num` string, " +
                "`split_original_amout` string, " +
                "`split_total_amount` string, " +
                "`split_activity_amount` string, " +
                "`split_coupon_amount` string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "`od_ts` string, " +
                "`oi_ts` string, " +
                "`row_op_ts` TIMESTAMP_LTZ(3), " +
                " primary key(`id`) not enforced " +
                ")" + MyKafkaUtils.getUpsertKafka("dwd_trade_order_pre_process"));

        tableEnv.executeSql("insert into dtopp select * from restable");
    //   env.execute();

    }
}
