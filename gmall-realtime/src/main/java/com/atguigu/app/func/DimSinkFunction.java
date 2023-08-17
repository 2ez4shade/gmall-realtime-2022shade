package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JedispoolUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @author: shade
 * @date: 2022/7/19 13:56
 * @description:
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
        jedisPool = JedispoolUtil.getJedisPool();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {


        String sinktable = value.getString("sinktable");
        String type = value.getString("type");
        String id = value.getJSONObject("data").getString("id");
        if ("update".equals(type)){
            Jedis jedis = jedisPool.getResource();
            DimUtil.delDimInfo(jedis, sinktable.toUpperCase(), id);
        }

        //sql: upsert table xxx.xxx(id,name) values('id','name')
        //编写sql
        String sql = genUpsertsql(sinktable, value.getJSONObject("data"));
        System.out.println(sql);




        //执行
        DruidPooledConnection connection = druidDataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        //手动提交 ,phoenix不自动提交
        connection.commit();

        //关闭资源
        preparedStatement.close();
        connection.close();
    }

    //sql: upsert table xxx.xxx(id,name) values('id','name')
    private String genUpsertsql(String sinktable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinktable + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";

    }


}
