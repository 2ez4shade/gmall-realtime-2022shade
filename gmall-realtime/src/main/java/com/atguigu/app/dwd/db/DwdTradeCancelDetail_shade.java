package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/23 20:51
 * @description:
 */
public class DwdTradeCancelDetail_shade {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table order_pre( " +
                "      id string, " +
                "      order_id string, " +
                "      sku_id string, " +
                "      sku_name string, " +
                "      order_price string, " +
                "      sku_num string, " +
                "      create_time string, " +
                "      source_type string, " +
                "      source_name string, " +
                "      source_id string, " +
                "      split_total_amount string, " +
                "      split_activity_amount string, " +
                "      split_coupon_amount string, " +
                "      consignee string, " +
                "      consignee_tel string, " +
                "      total_amount string, " +
                "      order_status string, " +
                "      user_id string, " +
                "      payment_way string, " +
                "      delivery_address string, " +
                "      order_comment string, " +
                "      out_trade_no string, " +
                "      trade_body string, " +
                "      operate_time string, " +
                "      expire_time string, " +
                "      process_status string, " +
                "      tracking_no string, " +
                "      parent_order_id string, " +
                "      province_id string, " +
                "      activity_reduce_amount string, " +
                "      coupon_reduce_amount string, " +
                "      original_total_amount string, " +
                "      feight_fee string, " +
                "      feight_fee_reduce string, " +
                "      refundable_time string, " +
                "      `old` Map<string,string>, " +
                "      activity_id string, " +
                "      activity_rule_id string, " +
                "      coupon_id string, " +
                "      coupon_use_id string " +
                ") " + MyKafkaUtils.getKafkaDDL("dwd_trade_order_pre_process","DwdTradeCancelDetail"));

        Table tableresult = tableEnv.sqlQuery("select " +
                "id, " +
                "order_id, " +
                "sku_id, " +
                "sku_name, " +
                "order_price, " +
                "sku_num, " +
                "create_time, " +
                "source_type, " +
                "source_name, " +
                "source_id, " +
                "split_total_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "consignee, " +
                "consignee_tel, " +
                "total_amount, " +
                "order_status, " +
                "user_id, " +
                "payment_way, " +
                "delivery_address, " +
                "order_comment, " +
                "out_trade_no, " +
                "trade_body, " +
                "operate_time, " +
                "expire_time, " +
                "process_status, " +
                "tracking_no, " +
                "parent_order_id, " +
                "province_id, " +
                "activity_reduce_amount, " +
                "coupon_reduce_amount, " +
                "original_total_amount, " +
                "feight_fee, " +
                "feight_fee_reduce, " +
                "refundable_time, " +
                "`old`, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                "coupon_use_id " +
                "from order_pre " +
                "where order_status = '1003' and `old` is not null and `old`['order_status'] is not null ");
        tableEnv.createTemporaryView("tableresult",tableresult);

        //TODO 建立kafka表 写出
        tableEnv.executeSql("create table kafkasinktable( " +
                "      id string, " +
                "      order_id string, " +
                "      sku_id string, " +
                "      sku_name string, " +
                "      order_price string, " +
                "      sku_num string, " +
                "      create_time string, " +
                "      source_type string, " +
                "      source_name string, " +
                "      source_id string, " +
                "      split_total_amount string, " +
                "      split_activity_amount string, " +
                "      split_coupon_amount string, " +
                "      consignee string, " +
                "      consignee_tel string, " +
                "      total_amount string, " +
                "      order_status string, " +
                "      user_id string, " +
                "      payment_way string, " +
                "      delivery_address string, " +
                "      order_comment string, " +
                "      out_trade_no string, " +
                "      trade_body string, " +
                "      operate_time string, " +
                "      expire_time string, " +
                "      process_status string, " +
                "      tracking_no string, " +
                "      parent_order_id string, " +
                "      province_id string, " +
                "      activity_reduce_amount string, " +
                "      coupon_reduce_amount string, " +
                "      original_total_amount string, " +
                "      feight_fee string, " +
                "      feight_fee_reduce string, " +
                "      refundable_time string, " +
                "      `old` Map<string,string>, " +
                "      activity_id string, " +
                "      activity_rule_id string, " +
                "      coupon_id string, " +
                "      coupon_use_id string " +
                ") " + MyKafkaUtils.getInsertKafka("dwd_trade_cancel_detail"));

        tableEnv.executeSql("insert into kafkasinktable select * from tableresult");

    }
}
