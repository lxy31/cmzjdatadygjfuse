package com.asiainfo.dygj.kafkautils;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * @ClassName MySchema
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/1/13 16:32
 * @Version 1.0
 **/
public class MySchema implements KeyedSerializationSchema<String> {
    @Override
    public byte[] serializeKey(String element) {
        //设置分区Key
        return element.getBytes();
    }

    @Override
    public byte[] serializeValue(String element) {
        return element.getBytes();
    }

    @Override
    public String getTargetTopic(String element) {
        return null;
    }
}
