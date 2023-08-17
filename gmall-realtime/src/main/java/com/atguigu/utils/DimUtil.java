package com.atguigu.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author: shade
 * @date: 2022/7/31 13:22
 * @description:
 */
public class DimUtil {

    public static JSONObject getDimInfo(Jedis jedis, Connection connection, String tablename, String key) throws Exception {
        //查缓存
        String rediskey = "DIM:" + tablename.toUpperCase() + ":" + key;
        String redisValue = jedis.get(rediskey);
        if (redisValue != null) {
            jedis.expire(rediskey, 24 * 60 * 60);
            return JSONObject.parseObject(redisValue);
        }

        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tablename.toUpperCase() + " where id = '" + key + "'";

        List<JSONObject> list = JdbcUtil.getFromJdbc(connection, sql, JSONObject.class, false);
        //存缓存
        JSONObject jsonObject = list.get(0);
        jedis.set(rediskey, jsonObject.toJSONString());

        return jsonObject;
    }

    public static void delDimInfo(Jedis jedis, String tablename, String key){
        String rediskey = "DIM:" + tablename.toUpperCase() + ":" + key;
        jedis.del(rediskey);
    }

    public static void main(String[] args) throws Exception {
        Jedis jedis = JedispoolUtil.getJedisPool().getResource();
        DruidPooledConnection connection = DruidDSUtil.createDataSource().getConnection();
        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(jedis, connection, "dim_base_trademark", "14");
        long end = System.currentTimeMillis();
        JSONObject dimInfo2 = getDimInfo(jedis, connection, "dim_base_trademark", "14");
        long end2 = System.currentTimeMillis();
        System.out.println(dimInfo);
        System.out.println(dimInfo2);
        System.out.println(end-start);
        System.out.println(end2-end);
    }
}
