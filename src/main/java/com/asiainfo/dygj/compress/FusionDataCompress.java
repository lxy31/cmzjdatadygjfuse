package com.asiainfo.dygj.compress;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.kafkautils.KafkaConsumer;
import com.asiainfo.dygj.kafkautils.KafkaProducer;
import com.asiainfo.dygj.monitor.process.FusionMonitor;
import com.asiainfo.dygj.monitor.process.MultiMonitor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @program: cmzjdatadygjfuse
 * @description: 剔除Topic：MULTIPLE_SOURCE_FUSION 5分钟内数据
 * @author: Mr.Deng -> Mr.Liu
 * @create: 2021-05-14 16:04
 **/
public class FusionDataCompress {
    private static long time = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
        final ParameterTool paraTool = ParameterTool.fromArgs(args);
        String consumerGroupId = paraTool.get("consumer.group.id", UUID.randomUUID().toString());
        System.out.println("consumerGroupId:" + consumerGroupId);

        /*获取MULTIPLE_SOURCE_FUSION数据，封装为SignalFormat类型*/
        DataStream<String> fusionStream = KafkaConsumer.createKafkaStream(env, "zz", "MULTIPLE_SOURCE_FUSION" + consumerGroupId, "MULTIPLE_SOURCE_FUSION", 60, SimpleStringSchema.class);
        SingleOutputStreamOperator<SignalFormat> formatDataStream = fusionStream.flatMap(new RichFlatMapFunction<String, SignalFormat>() {
            private transient Counter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.counter = getRuntimeContext().getMetricGroup()
                        .counter("myCounterSource");
            }

            @Override
            public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
                String[] fusionStrs = s.split(",", -1);
                if (fusionStrs.length == 15) {
                    this.counter.inc();
                    SignalFormat fusionData = SignalFormat.of(fusionStrs[0], fusionStrs[1], fusionStrs[2], fusionStrs[3], fusionStrs[4],
                            fusionStrs[5], fusionStrs[6], fusionStrs[7], fusionStrs[8], fusionStrs[9],
                            fusionStrs[10], fusionStrs[11], fusionStrs[12], fusionStrs[13], fusionStrs[14]);
                    collector.collect(fusionData);
                }
            }
        }).name("KafkaFlatMapData");

        /*5分钟清除数据库中过期状态*/
        SingleOutputStreamOperator<SignalFormat> fuseReduceStream = formatDataStream.filter(new PhoneTimeIntervalFilterFunction()).name("FusionFilter");

        /*统计入Topic:STREAM_CENTER_TRACE_MINUTER的数据量情况*/
//        fuseReduceStream.addSink(new FusionMonitor());

        SingleOutputStreamOperator<String> stringStream = fuseReduceStream.map(new RichMapFunction<SignalFormat, String>() {
            private transient Counter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.counter = getRuntimeContext().getMetricGroup()
                        .counter("myCounterSink");
            }

            @Override
            public String map(SignalFormat signalFormat) throws Exception {
                this.counter.inc();
                return signalFormat.toString();
            }
        }).name("FusionMap");

        KafkaProducer.createKafkaSink(stringStream, "jc", "STREAM_CENTER_TRACE_MINUTER");
        env.execute("FusionDataCompress");
    }
}
