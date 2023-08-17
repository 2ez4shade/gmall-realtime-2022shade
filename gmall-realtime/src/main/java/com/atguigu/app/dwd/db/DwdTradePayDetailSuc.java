package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import com.atguigu.utils.MySqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/25 20:13
 * @description:
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table restable( " +
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
                "`split_original_amount` string, " +
                "`split_total_amount` string, " +
                "`split_activity_amount` string, " +
                "`split_coupon_amount` string, " +
                "`ts` string, " +
                "`row_op_ts` TIMESTAMP_LTZ(3) " +
                ")" + MyKafkaUtils.getKafkaDDL("dwd_trade_order_detail", "DwdTradePayDetailSuc"));

        tableEnv.executeSql(MyKafkaUtils.getTopicdbDDL("DwdTradePayDetailSuc"));
        tableEnv.executeSql(MySqlUtils.getBaseDic());

        Table payment_info = tableEnv.sqlQuery("select " +
                "`data`['user_id'] user_id, " +
                "`data`['order_id'] order_id, " +
                "`data`['payment_type'] payment_type, " +
                "`data`['callback_time'] callback_time, " +
                "`pt`, " +
                "`ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `data`['payment_status']='1602' " + "and `old`['payment_status'] is not null"
        );
        tableEnv.createTemporaryView("payment_info", payment_info);

        Table table = tableEnv.sqlQuery("select " +
                "od.id order_detail_id,  " +
                "od.order_id,  " +
                "od.user_id,  " +
                "od.sku_id,  " +
                "od.sku_name,  " +
                "od.province_id,  " +
                "od.activity_id,  " +
                "od.activity_rule_id,  " +
                "od.coupon_id,  " +
                "pi.payment_type payment_type_code,  " +
                "dic.dic_name payment_type_name,  " +
                "pi.callback_time,  " +
                "od.source_id,  " +
                "od.source_type,  " +
                "od.source_type_name,  " +
                "od.sku_num,  " +
                "od.split_original_amount,  " +
                "od.split_activity_amount,  " +
                "od.split_coupon_amount,  " +
                "od.split_total_amount split_payment_amount,  " +
                "pi.ts,  " +
                "od.row_op_ts row_op_ts   " +
                "from restable od " +
                "join payment_info pi " +
                "on od.order_id=pi.order_id " +
                "join base_dic for system_time as of pi.pt dic " +
                "on dic.dic_code=pi.payment_type");
        tableEnv.createTemporaryView("table", table);

        tableEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(order_detail_id) not enforced " +
                ")" + MyKafkaUtils.getUpsertKafka("dwd_trade_pay_detail_suc"));
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from `table`");

    }
}
