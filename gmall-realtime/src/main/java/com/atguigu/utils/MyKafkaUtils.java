package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @author: shade
 * @date: 2022/7/18 19:39
 * @description:
 */
public class MyKafkaUtils {
    private  static Properties properties = new Properties();
    static {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){


        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record==null||record.value()==null){
                    return null;
                }else {
                    return new String(record.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        },properties);
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static String getKafkaDDL(String topic,String groupid){
        return "with( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '"+topic+"', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = '"+groupid+"', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ")";
    }
    public static String getInsertKafka(String topic){
        return "with( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '"+topic+"', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'format' = 'json' " +
                ")";
    }

    public static String getUpsertKafka(String topic){
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '"+topic+"', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
    
    public static String getTopicdbDDL(String groupid){
        return "create table topic_db( " +
                "      `database` string, " +
                "      `table` string, " +
                "      `type` string, " +
                "      `data` map<string,string>, " +
                "      `old` map<string,string>, " +
                "      `ts` string, " +
                "      `pt` as proctime()" +
                ")with( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'topic_db', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = '" + groupid + "', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ")";
    }

}
