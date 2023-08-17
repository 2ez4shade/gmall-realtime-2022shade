package com.atguigu.app.dws;

import com.atguigu.app.func.ikword;
import com.atguigu.bean.KeywordBean;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: shade
 * @date: 2022/7/25 17:07
 * @description:
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 从dwd_traffic_page_log中读取数据
        tableEnv.executeSql("create table pagetable( " +
                "`common` map<string,string>, " +
                "`page` map<string,string>, " +
                "`ts` bigint, " +
                "`rt` as TO_TIMESTAMP_LTZ(ts,3), " +
                "  WATERMARK FOR `rt` AS `rt` - INTERVAL '3' SECOND " +
                ")" + MyKafkaUtils.getKafkaDDL("dwd_traffic_page_log", "DwsTrafficSourceKeywordPageViewWindow"));
//        tableEnv.executeSql("create table pagetable1( " +
//                "`common` map<string,string>, " +
//                "`page` map<string,string>, " +
//                "`ts` bigint " +
//                ")" + MyKafkaUtils.getKafkaDDL("dwd_traffic_page_log", "DwsTrafficSourceKeywordPageViewWindow"));
//        tableEnv.sqlQuery("select * from pagetable1").execute().print();
        //todo 过滤出搜索行为
        Table filltertable = tableEnv.sqlQuery("select " +
                "`page`['item'] fullword, " +
                "`rt` " +
                "from pagetable " +
                "where `page`['item'] is not null " +
                "and `page`['last_page_id'] = 'search' " +
                "and `page`['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("filltertable", filltertable);

        //todo 使用自定义udtf侧写
        tableEnv.createTemporaryFunction("ikword", ikword.class);
        Table udtftable = tableEnv.sqlQuery("select " +
                "`keyword`, " +
                "`rt` " +
                "from filltertable " +
                "LEFT JOIN LATERAL TABLE(ikword(fullword)) AS T(keyword) ON TRUE ");
        tableEnv.createTemporaryView("udtftable", udtftable);

        //todo 开窗聚合
        Table grouptable = tableEnv.sqlQuery("select " +
                "date_format(TUMBLE_START(`rt`, INTERVAL '10' SECOND),'yyyy-MM-dd HH-mm-ss') stt, " +
                "date_format(TUMBLE_END(`rt`, INTERVAL '10' SECOND),'yyyy-MM-dd HH-mm-ss') edt, " +
                "'"+GmallConstant.KEYWORD_SEARCH+"'"+ " source, " +
                "`keyword`, " +
                "count(*) keyword_count , " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from udtftable " +
                "group by keyword , " +
                "TUMBLE(`rt`, INTERVAL '10' SECOND)");
        tableEnv.createTemporaryView("grouptable",grouptable);

        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(grouptable, KeywordBean.class);
        keywordBeanDataStream.print();

        //todo 写到clickhouse
        keywordBeanDataStream.addSink(ClickHouseUtil.getJdbcSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        //todo 启动
        env.execute();
    }
}
