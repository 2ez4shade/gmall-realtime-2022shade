package com.atguigu.utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author: shade
 * @date: 2022/7/31 12:46
 * @description:
 */
public class JedispoolUtil {
    private static JedisPool jedisPool;

    private static void init() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 10000);
    }

    public static JedisPool getJedisPool() {
        if (jedisPool == null) {
            init();
        }
        return jedisPool;

    }

}
