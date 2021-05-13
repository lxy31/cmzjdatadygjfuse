package com.asiainfo.dygj.kafkautils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/***********************************
 *@Desc TODO
 *@ClassName KafkaConsumer
 *@Author DLX
 *@Data 2020/7/9 10:50
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KafkaConsumer {
    public static <T> DataStream<T> createKafkaStream(StreamExecutionEnvironment env, String consumerBootServerType, String consumerGroupId, String consumerTopic, int parallelism, Class<? extends DeserializationSchema<T>> clazz) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Properties consumerProperties = new Properties();
        if (consumerBootServerType.equals("zz")) {
            consumerProperties.setProperty("bootstrap.servers", "10.76.183.77:21007,10.76.183.78:21007,10.76.183.79:21007");
            consumerProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
            consumerProperties.setProperty("kerberos.domain.name", "hadoop.HD_JHJCFX.COM");
            consumerProperties.setProperty("sasl.kerberos.service.name", "kafka");
            System.out.println("consumerBootServer:10.76.183.77:21007");
        } else if (consumerBootServerType.equals("jc")) {
            consumerProperties.setProperty("bootstrap.servers", "10.78.142.16:21005,10.70.58.152:21005,10.70.59.140:21005");
            System.out.println("consumerBootServer:10.78.142.16:21005");
        }
        consumerProperties.setProperty("group.id", consumerGroupId);
        consumerProperties.setProperty("buffer.memory", "558345748");
        consumerProperties.setProperty("max.request.size", "10485760");
        consumerProperties.setProperty("max.poll.records", "20000");
        consumerProperties.setProperty("max.partition.fetch.bytes", "10485760");
        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011<T>(consumerTopic,
                clazz.getConstructor().newInstance(),
                consumerProperties);
        return env.addSource(kafkaConsumer).setParallelism(parallelism).name(consumerTopic);
    }
}
