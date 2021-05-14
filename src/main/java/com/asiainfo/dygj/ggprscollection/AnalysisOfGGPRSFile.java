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

/**
 * @program: cmzjdatadygjfuse
 * @description: 读取FTP（10.78.142.132 22）（10.78.142.154 22）上的数据
 * @author: Mr.Deng -> Mr.Liu
 * @create: 2021-05-14 16:04
 **/
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
        /*定于全局参数，便于参数传递*/
        String ftpType = paraTool.get("ftp.type.id", "0");
        int sourceParallelism = paraTool.getInt("ftp.source.parallelism", 9);

        SingleOutputStreamOperator<String> line = env.addSource(new GGPRSFileSource()).setParallelism(sourceParallelism).startNewChain();
        //生产topic
//        line.addSink(new GGRPSMonitor());
        KafkaProducer.createKafkaSink(line, "jc", "topic_ggprs");
        env.execute("AnalysisOfGGPRSFile");
    }
}
