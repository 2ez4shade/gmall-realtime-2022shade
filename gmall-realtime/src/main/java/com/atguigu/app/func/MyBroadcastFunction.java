package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/19 10:34
 * @description:
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //cdd读取来的信息表string
//        {"before":null,"after":{"id":19,"name":"das"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source",
//        "ts_ms":1658198477000,"snapshot":"false","db":"gmall2021","sequence":null,"table":"base_category1","server_id":1,"gtid":null,
//        "file":"mysql-bin.000074","pos":1790,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1658198478504,"transaction":null}
        //after字段转为json
        JSONObject jsonObject = JSONObject.parseObject(value);
        JSON.parseObject(jsonObject.getJSONObject("after"), TableProcess.class)

        //建表

        //写入状态

    }
}
