package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author: shade
 * @date: 2022/7/25 19:41
 * @description:
 */
public class DwdTradeCancelDetail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的保存时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

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
                "`row_op_ts` TIMESTAMP_LTZ(3) " +
                ")" + MyKafkaUtils.getKafkaDDL("dwd_trade_order_pre_process", "DwdTradeCancelDetail"));

        Table CancelDetail = tableEnv.sqlQuery("select " +
                "`id`, " +
                "`order_id`, " +
                "`user_id`, " +
                "`order_status`, " +
                "`sku_id`, " +
                "`sku_name`, " +
                "`province_id`, " +
                "`activity_id`, " +
                "`activity_rule_id`, " +
                "`coupon_id`, " +
                "`date_id`, " +
                "`create_time`, " +
                "`operate_date_id`, " +
                "`operate_time`, " +
                "`source_type`, " +
                "`source_id`, " +
                "`source_type_name`, " +
                "`sku_num`, " +
                "`split_original_amout`, " +
                "`split_total_amount`, " +
                "`split_activity_amount`, " +
                "`split_coupon_amount`, " +
                "`oi_ts` ts, " +
                "`row_op_ts` " +
                "from  " +
                "dtopp " +
                "where `type`='update' " +
                "and `order_status`='1003' " +
                "and `old` is not null and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("CancelDetail",CancelDetail);

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
                "`split_original_amout` string, " +
                "`split_total_amount` string, " +
                "`split_activity_amount` string, " +
                "`split_coupon_amount` string, " +
                "`ts` string, " +
                "`row_op_ts` TIMESTAMP_LTZ(3) " +
                ")" + MyKafkaUtils.getInsertKafka("dwd_trade_cancel_detail"));
        tableEnv.executeSql("insert into restable select * from CancelDetail");
    }
}
