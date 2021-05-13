package com.asiainfo.dygj.ggprscollection;

import com.asiainfo.dygj.kafkautils.KafkaProducer;
import com.asiainfo.dygj.monitor.process.GGRPSMonitor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/***********************************
 *@Desc TODO
 *@ClassName AnalysisOfGGPRSFile
 *@Author DLX
 *@Data 2020/8/11 10:21
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class AnalysisOfGGPRSFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
        final ParameterTool paraTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(paraTool);
        String ftpType = paraTool.get("ftp.type.id", "0");
        int sourceParallelism = paraTool.getInt("ftp.source.parallelism", 9);
        SingleOutputStreamOperator<String> line = env.addSource(new GGPRSFileSource()).setParallelism(sourceParallelism).startNewChain();
        //生产topic
//        line.addSink(new GGRPSMonitor());
        KafkaProducer.createKafkaSink(line, "jc", "topic_ggprs");
        env.execute("AnalysisOfGGPRSFile");
    }
}
