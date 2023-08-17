package com.atguigu.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: shade
 * @date: 2022/7/31 13:05
 * @description:
 */
public class JdbcUtil {


    public static <T> List<T> getFromJdbc(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        ArrayList<T> list = new ArrayList<>();

        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                T t = clz.newInstance();

                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object object = resultSet.getObject(columnName);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(t, columnName, object);
                }

                list.add(t);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        return list;
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
        String sql = "select * from  GMALL2022_REALTIME.DIM_BASE_TRADEMARK";
        List<JSONObject> jsonObjects = getFromJdbc(connection, sql, JSONObject.class, true);
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
    }
}
