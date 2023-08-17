package com.atguigu.utils;

/**
 * @author: shade
 * @date: 2022/7/22 19:55
 * @description:
 */
public class MySqlUtils {
    public static String getBaseDic(){
        return "CREATE TEMPORARY TABLE base_dic ( " +
                "  dic_code STRING, " +
                "  dic_name STRING, " +
                "  parent_code STRING, " +
                "  create_time STRING, " +
                "  operate_time STRING " +
                ")" + getLookUpDDL("base_dic");
    }

    public static String getLookUpDDL(String table){
        return " WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'table-name' = '"+table+"', " +
                "  'username' = 'root', " +
                "  'password' = '123456', " +
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour' " +
                ")";
    }
}
