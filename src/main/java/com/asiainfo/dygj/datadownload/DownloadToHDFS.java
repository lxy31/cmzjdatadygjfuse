package com.asiainfo.dygj.datadownload;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.kafkautils.KafkaConsumer;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/***********************************
 *@Desc TODO
 *@ClassName DownloadToHDFS
 *@Author DLX
 *@Data 2020/9/11 10:14
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class DownloadToHDFS {
//    com.asiainfo.dygj.datadownload.DownloadToHDFS
    private static final OutputTag<SignalFormat> outputTag = new OutputTag<SignalFormat>("LateData"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
        final ParameterTool paraTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(paraTool);
        String basePath = paraTool.get("save.hdfs.path","hdfs://nsfed/ns2/jc_zz_swrz/dygjoutput/MULTIPLE_SOURCE_FUSION_test/");
        long sinkSize = Long.valueOf(paraTool.get("sink.file.size","1024"));
        String consumerGroupId = paraTool.get("consumer.group.id", UUID.randomUUID().toString());
        System.out.println("consumerGroupId:"+consumerGroupId);

        DataStream<String> fusionStream = KafkaConsumer.createKafkaStream(env, "zz", "MULTIPLE_SOURCE_FUSION_test" + consumerGroupId, "MULTIPLE_SOURCE_FUSION_test", 60, SimpleStringSchema.class);

        SingleOutputStreamOperator<SignalFormat> downloadDataStream = fusionStream.flatMap(new FlatMapFunction<String, SignalFormat>() {
            @Override
            public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
                String[] fusionStrs = s.split(",", -1);
                if (fusionStrs.length == 15 && !fusionStrs[1].equals("7")){
                    SignalFormat fusionData = SignalFormat.of(fusionStrs[0], fusionStrs[1], fusionStrs[2], fusionStrs[3], fusionStrs[4],
                            fusionStrs[5], fusionStrs[6], fusionStrs[7], fusionStrs[8], fusionStrs[9],
                            fusionStrs[10], fusionStrs[11], fusionStrs[12], fusionStrs[13], fusionStrs[14]);
                    fusionData.signalTime = TimeUtil.stampToDate(TimeUtil.dateToStamp(fusionData.signalTime, "yyyyMMddHHmmss"), "yyyy-MM-dd HH");
                    collector.collect(fusionData);
                }
            }
        });

        SingleOutputStreamOperator<String> filterLateDataStream = downloadDataStream.process(new ProcessFunction<SignalFormat, String>() {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");

            @Override
            public void processElement(SignalFormat s, Context context, Collector<String> collector) throws Exception {
                boolean flag = df.format(new Date()).equals(s.signalTime);
                if (flag) {
                    collector.collect(s.downloadDataToString());
                } else {
                    context.output(outputTag, s);
                }
            }
        }).startNewChain();

        DataStream<SignalFormat> lateDataStream = filterLateDataStream.getSideOutput(outputTag);

        BucketingSink<String> sink = new BucketingSink<String>(basePath);
        sink.setBucketer(new DateTimeBucketer<String>("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));

        sink.setBatchSize(sinkSize * 1024 * 1024L); // MB
        sink.setBatchRolloverInterval(3600000); // this is 60 mins
        sink.setPendingPrefix("FUSION");
        sink.setPendingSuffix("Data");
        sink.setInProgressPrefix(".");

        filterLateDataStream.addSink(sink).name("HdfsSink");
        lateDataStream.addSink(new LateDataSink()).name("LateDataSink");
        lateDataStream.print();

        env.execute("DownloadToHDFS");
    }
}
