package com.test;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSink;
import org.apache.rocketmq.flink.common.selector.DefaultTopicSelector;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema.DEFAULT_KEY_FIELD;
import static org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema.DEFAULT_VALUE_FIELD;

public class HDFS2RocketMQFlinkStream160 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);

        tableEnv.connect(new FileSystem().path("D:/workspace201908/flinkrocketmq/data/2019-08-03"))
                .withSchema(
                        new Schema()
                                .field("name", Types.STRING)
                                .field("age", Types.STRING)
                                .field("sex", Types.STRING)
                                .field("id", Types.STRING)
                )
                .withFormat(
                        new Csv()
                                .field("name", Types.STRING)
                                .field("age", Types.STRING)
                                .field("sex", Types.STRING)
                                .field("id", Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("original");

        Table table = tableEnv.sqlQuery("select * from original");
        SingleOutputStreamOperator<Map<String, String>> ds = tableEnv.toAppendStream(table, Row.class)
                .map(new MapFunction<Row, Map<String, String>>() {
                    @Override
                    public Map<String, String> map(Row row) throws Exception {
                        String name = (String) row.getField(0);
                        String age = (String) row.getField(1);
                        String sex = (String) row.getField(2);
                        String id = (String) row.getField(3);
                        JsonObject obj = new JsonObject();
                        obj.addProperty("name", name);
                        obj.addProperty("age", age);
                        obj.addProperty("sex", sex);
                        obj.addProperty("id", id);
                        String value = obj.toString();
                        System.out.println(value);
                        String key = "uuid_" + System.currentTimeMillis();
                        HashMap<String, String> map = new HashMap<>();
                        map.put(DEFAULT_KEY_FIELD, key);
                        map.put(DEFAULT_VALUE_FIELD, value);
                        return map;
                    }
                });

        ds.print();

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        int msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05;
        producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel));

        RocketMQSink sink = new RocketMQSink(new SimpleKeyValueSerializationSchema()
                , new DefaultTopicSelector("flink-hdfs-rmq-new2")
                , producerProps)
                .withBatchFlushOnCheckpoint(true)
                .withAsync(true)
                .withBatchSize(100);

        ds.addSink(sink).name("rocketmq-sink");

        sEnv.execute("HDFS2RocketMQFlinkStream");
    }
}
