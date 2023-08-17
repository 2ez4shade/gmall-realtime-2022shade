package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author: shade
 * @date: 2022/7/19 10:34
 * @description:
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private DruidDataSource druidDataSource;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableprocess) {
        mapStateDescriptor = tableprocess;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //{"database":"gmall","table":"comment_info","type":"insert","ts":1592219975,"xid":1098,"commit":true,"data":{"id":1548990863326093318,"user_id":11,
        // "nick_name":null,"head_img":null,"sku_id":12,"spu_id":3,"order_id":7,"appraise":"1204",
        // "comment_txt":"评论内容：27497794931231945394397519357356156493496626236664","create_time":"2020-06-14 19:19:35","operate_time":null}}
        //获取广播流状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));
        String type = value.getString("type");

        if (tableProcess!=null&&(type.equals("insert")||type.equals("update")||type.equals("bootstrap-insert"))){
            //过滤字段
            fillterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //添加sinktable字段,写出
            value.put("sinktable", tableProcess.getSinkTable());

            out.collect(value);
        }
    }

    private void fillterColumn(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> columns = Arrays.asList(split);

        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        entrySet.removeIf(entry -> !columns.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //cdd读取来的信息表string
//        {"before":null,"after":{"id":19,"name":"das"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source",
//        "ts_ms":1658198477000,"snapshot":"false","db":"gmall2021","sequence":null,"table":"base_category1","server_id":1,"gtid":null,
//        "file":"mysql-bin.000074","pos":1790,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1658198478504,"transaction":null}
        //after字段转为json
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //建表
        checktable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());

        //写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);

    }

    private void checktable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //create table xxx.xxx(id varchar primary key, name varchar)xxx
        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //拼接属性
            String[] split = sinkColumns.split(",");
            for (int i = 0; i < split.length; i++) {
                String column = split[i];
                if (column.equals(sinkPk)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }
                if (i < split.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);
            System.out.println(sql.toString());

            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("--------------hbase中创建" + sinkTable + "表失败!!!!------------");
        } finally {
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
