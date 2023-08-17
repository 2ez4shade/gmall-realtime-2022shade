package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JedispoolUtil;
import com.atguigu.utils.ThreadpoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author: shade
 * @date: 2022/7/31 12:59
 * @description:
 */
public abstract class DwsAsyncFunction<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T>{
    private DruidDataSource druidDataSource;
    private JedisPool jedisPool;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tablename;

    public DwsAsyncFunction(String tablename) {
        this.tablename = tablename;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
        jedisPool = JedispoolUtil.getJedisPool();
        threadPoolExecutor = ThreadpoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                DruidPooledConnection connection = druidDataSource.getConnection();
                Jedis jedis = jedisPool.getResource();
                //抽象方法获取key
                String key = getKey(input);
                //获取hbase中对应表,对应主键的数据
                JSONObject dimInfo = DimUtil.getDimInfo(jedis, connection, tablename, key);

                join(input, dimInfo);

                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }
}
