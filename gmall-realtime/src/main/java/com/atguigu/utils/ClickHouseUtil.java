package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import com.atguigu.common.GmallConstant;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.beans.Transient;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author: shade
 * @date: 2022/7/26 14:46
 * @description:
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.sink(sql
                , new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Class<?> clz = t.getClass();
                        Field[] fields = clz.getDeclaredFields();

                        int j = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);

                            //判断是否是transient
                            TransientSink transientSink = field.getAnnotation((TransientSink.class));
                            if (transientSink != null) {
                                j++;
                                continue;
                            }
                            Object obj = field.get(t);
                            preparedStatement.setObject(i + j + 1, obj);
                        }
                    }
                }
                , new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
