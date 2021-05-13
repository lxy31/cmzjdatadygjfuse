package com.asiainfo.dygj.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;
import java.util.UUID;

/***********************************
 *@Desc TODO
 *@ClassName KafkaSourceAndSink
 *@Author DLX
 *@Data 2020/8/12 10:04
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class KafkaClint {
//    com.asiainfo.dygj.util.KafkaClint
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String consumerBootServer = paraTool.get("consumer.servers.type", "zz");
        String consumerGroupId = paraTool.get("consumer.group.id", UUID.randomUUID().toString());
        String consumerTopic = paraTool.getRequired("consumer.topic");
        String output = paraTool.get("hdfs.data.output",null);

        System.out.println("consumerGroupId:"+consumerGroupId);

        int consumerParallelism = Integer.parseInt(paraTool.get("consumer.parallelism","1"));
        //KafkaSource配置
        Properties consumerProperties = new Properties();
        if (consumerBootServer.equals("zz")){
            consumerProperties.setProperty("bootstrap.servers", "10.76.183.77:21007,10.76.183.78:21007,10.76.183.79:21007");
            consumerProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
            consumerProperties.setProperty("kerberos.domain.name", "hadoop.HD_JHJCFX.COM");
            consumerProperties.setProperty("sasl.kerberos.service.name", "kafka");
            System.out.println("consumerBootServer:10.76.183.77:21007");
        }else if (consumerBootServer.equals("jcfx")){
            consumerProperties.setProperty("bootstrap.servers", "10.78.142.16:21005,10.70.58.152:21005,10.70.59.140:21005");
            System.out.println("consumerBootServer:10.78.142.16:21005");
        }
        consumerProperties.setProperty("group.id", consumerGroupId);
        consumerProperties.setProperty("buffer.memory", "558345748");
        consumerProperties.setProperty("max.request.size", "10485760");
        consumerProperties.setProperty("max.poll.records", "20000");
        consumerProperties.setProperty("max.partition.fetch.bytes", "10485760");
        //kafkaSource
        DataStream<String> lines = env.addSource(new FlinkKafkaConsumer011<>(consumerTopic,
                new SimpleStringSchema(),
                consumerProperties)
        ).setParallelism(consumerParallelism);
        SingleOutputStreamOperator<String> mapStream = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).startNewChain();

        mapStream.print();
        if (output!=null){
            System.out.println("hdfs.data.output:"+output);
            mapStream.writeAsText(output);
        }
        env.execute("KafkaClint");
    }
}
