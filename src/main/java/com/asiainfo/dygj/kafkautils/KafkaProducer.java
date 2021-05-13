package com.asiainfo.dygj.kafkautils;

import com.asiainfo.dygj.bean.SignalFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


import java.util.Properties;

/***********************************
 *@Desc TODO
 *@ClassName KafkaProducer
 *@Author DLX
 *@Data 2020/8/20 15:48
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KafkaProducer {
    public static DataStreamSink<String> createKafkaSink(SingleOutputStreamOperator<String> dataOutPutStream, String producerBootServerType, String producerTopic) throws IllegalAccessException, InstantiationException {
        Properties producerProperties = new Properties();
        if (producerBootServerType.equals("zz")) {
            producerProperties.setProperty("bootstrap.servers", "10.76.183.77:21007,10.76.183.78:21007,10.76.183.79:21007");
            producerProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
            producerProperties.setProperty("kerberos.domain.name", "hadoop.HD_JHJCFX.COM");
            producerProperties.setProperty("sasl.kerberos.service.name", "kafka");
            System.out.println("producerBootServer:10.76.183.77:21007");
        } else if (producerBootServerType.equals("jc")) {
            producerProperties.setProperty("bootstrap.servers", "10.78.142.16:21005,10.70.58.152:21005,10.70.59.140:21005");
            System.out.println("producerBootServer:10.78.142.16:21005");
        }
        producerProperties.setProperty("acks", "1");
        producerProperties.setProperty("retries", "1000000000");
        producerProperties.setProperty("max_in_flight_requests_per_connection", "1");
        producerProperties.setProperty("request.timeout.ms", "1000000000");
        producerProperties.setProperty("retry.backoff.ms", "20000");
        producerProperties.setProperty("batch.size", "1048576");
        producerProperties.setProperty("buffer.memory", "558345748");
        producerProperties.setProperty("max.request.size", "10485760");
        producerProperties.setProperty("linger.ms", "500");

        FlinkKafkaProducer010<String> kafkaProducer = new FlinkKafkaProducer010<>(
                producerTopic,
//                new SimpleStringSchema(),
                //GGPRS_Collection
                new MySchema(),
                producerProperties,
                new MyKafkaPartitioner());

        return dataOutPutStream.addSink(kafkaProducer);
    }
}
