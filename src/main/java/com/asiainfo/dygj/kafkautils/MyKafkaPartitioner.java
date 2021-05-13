package com.asiainfo.dygj.kafkautils;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @ClassName MyKafkaPartitioner
 * @Description //自定义flink沉槽Kafka的分区信息
 * @Author 刘晓雨
 * @Date 2021/1/13 15:20
 * @Version 1.0
 **/
public class MyKafkaPartitioner extends FlinkKafkaPartitioner<String> {

    /**
     * @Author 刘晓雨
     * @Description //TODO
     * @Date 15:33 2021/1/13
     * @Param [record:  记录
     *         key:     KeyedSerializationSchema -> key
     *         value:   KeyedSerializationSchema -> value
     *         targetTopic: 主题
     *         partitions:  分区
     *         ]
     * @return [java.lang.String, byte[], byte[], java.lang.String, int[]]
     **/
    @Override
    public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return Math.abs(new String(key).hashCode() % partitions.length);
    }
}
