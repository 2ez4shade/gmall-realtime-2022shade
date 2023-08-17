package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import com.atguigu.utils.MySqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/23 11:25
 * @description:
 */
public class DwdTradeOrderPreProcess_shade {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO kafka读取topicdb
        tableEnv.executeSql(MyKafkaUtils.getTopicdbDDL("DwdTradeOrderPreProcess"));

        //TODO 查询出订单表
        Table order_info = tableEnv.sqlQuery("" +
                "select " +
                "`data`['id'] id, " +
                "`data`['consignee'] consignee, " +
                "`data`['consignee_tel'] consignee_tel, " +
                "`data`['total_amount'] total_amount, " +
                "`data`['order_status'] order_status, " +
                "`data`['user_id'] user_id, " +
                "`data`['payment_way'] payment_way, " +
                "`data`['delivery_address'] delivery_address, " +
                "`data`['order_comment'] order_comment, " +
                "`data`['out_trade_no'] out_trade_no, " +
                "`data`['trade_body'] trade_body, " +
                "`data`['create_time'] create_time, " +
                "`data`['operate_time'] operate_time, " +
                "`data`['expire_time'] expire_time, " +
                "`data`['process_status'] process_status, " +
                "`data`['tracking_no'] tracking_no, " +
                "`data`['parent_order_id'] parent_order_id, " +
                "`data`['province_id'] province_id, " +
                "`data`['activity_reduce_amount'] activity_reduce_amount, " +
                "`data`['coupon_reduce_amount'] coupon_reduce_amount, " +
                "`data`['original_total_amount'] original_total_amount, " +
                "`data`['feight_fee'] feight_fee, " +
                "`data`['feight_fee_reduce'] feight_fee_reduce, " +
                "`data`['refundable_time'] refundable_time, " +
                "`old` " +
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_info' " +
                "and (`type`='insert' or `type`='update')");
        tableEnv.createTemporaryView("order_info",order_info);
  //      tableEnv.toAppendStream(order_info, Row.class).print();

        //TODO 查询出订单明细表
        Table order_detail = tableEnv.sqlQuery("" +
                "select " +
                "`data`['id'] id, " +
                "`data`['order_id'] order_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['sku_name'] sku_name, " +
                "`data`['order_price'] order_price, " +
                "`data`['sku_num'] sku_num, " +
                "`data`['create_time'] create_time, " +
                "`data`['source_type'] source_type, " +
                "`data`['source_id'] source_id, " +
                "`data`['split_total_amount'] split_total_amount, " +
                "`data`['split_activity_amount'] split_activity_amount, " +
                "`data`['split_coupon_amount'] split_coupon_amount, " +
                "`pt` " +
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_detail' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail",order_detail);
 //       tableEnv.toAppendStream(order_detail, Row.class).print();

        //TODO 查询出活动表
        Table order_detail_activity = tableEnv.sqlQuery("select " +
                "`data`['id'] id, " +
                "`data`['order_id'] order_id, " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['activity_id'] activity_id, " +
                "`data`['activity_rule_id'] activity_rule_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['create_time'] create_time " +
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",order_detail_activity);
    //    tableEnv.toAppendStream(order_detail_activity, Row.class).print();


        //TODO 查询出购物券表
        Table order_detail_coupon = tableEnv.sqlQuery("select " +
                "`data`['id'] id, " +
                "`data`['order_id'] order_id, " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['coupon_id'] coupon_id, " +
                "`data`['coupon_use_id'] coupon_use_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['create_time'] create_time " +
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon",order_detail_coupon);
  //      tableEnv.toAppendStream(order_detail_coupon,Row.class).print();

        //TODO lookup mysql base_dic
        tableEnv.executeSql(MySqlUtils.getBaseDic());

        //TODO 5表关联
        Table result = tableEnv.sqlQuery("select " +
                "      od.id, " +
                "      od.order_id, " +
                "      od.sku_id, " +
                "      od.sku_name, " +
                "      od.order_price, " +
                "      od.sku_num, " +
                "      od.create_time, " +
                "      od.source_type, " +
                "      dic.dic_name source_name, " +
                "      od.source_id, " +
                "      od.split_total_amount, " +
                "      od.split_activity_amount, " +
                "      od.split_coupon_amount, " +
                "      oi.consignee, " +
                "      oi.consignee_tel, " +
                "      oi.total_amount, " +
                "      oi.order_status, " +
                "      oi.user_id, " +
                "      oi.payment_way, " +
                "      oi.delivery_address, " +
                "      oi.order_comment, " +
                "      oi.expire_time, " +
                "      oi.out_trade_no, " +
                "      oi.trade_body, " +
                "      oi.operate_time, " +
                "      oi.process_status, " +
                "      oi.tracking_no, " +
                "      oi.parent_order_id, " +
                "      oi.province_id, " +
                "      oi.activity_reduce_amount, " +
                "      oi.coupon_reduce_amount, " +
                "      oi.original_total_amount, " +
                "      oi.feight_fee, " +
                "      oi.feight_fee_reduce, " +
                "      oi.refundable_time, " +
                "      oi.`old`, " +
                "      oa.activity_id, " +
                "      oa.activity_rule_id, " +
                "      oc.coupon_id, " +
                "      oc.coupon_use_id " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id=oi.id " +
                "left join order_detail_activity oa  " +
                "on od.id=oa.order_detail_id " +
                "left join order_detail_coupon oc  " +
                "on oc.order_detail_id=od.id " +
                "join base_dic for SYSTEM_TIME as of od.pt as dic " +
                "on od.source_type=dic.dic_code");
        tableEnv.createTemporaryView("resultable",result);
        tableEnv.toChangelogStream(result).print(">>>>>");

        //TODO 建立upset_kafka表 写出
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
                "      coupon_use_id string, " +
                "      PRIMARY KEY (id) NOT ENFORCED " +
                ") " + MyKafkaUtils.getUpsertKafka("dwd_trade_order_pre_process"));
        //写出
        tableEnv.executeSql("upsert into order_pre select  *  from  resultable");

       env.execute();

    }
}
