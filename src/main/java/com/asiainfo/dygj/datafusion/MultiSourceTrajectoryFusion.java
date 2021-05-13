package com.asiainfo.dygj.datafusion;

import com.asiainfo.dygj.bean.BaseStationInfo;
import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.filterfunc.PhoneTimeFilterFunction;
import com.asiainfo.dygj.kafkautils.KafkaConsumer;
import com.asiainfo.dygj.kafkautils.KafkaProducer;
import com.asiainfo.dygj.mapfunc.*;
import com.asiainfo.dygj.monitor.process.MultiMonitor;
import com.asiainfo.dygj.sourcefunc.OracleCustomSource;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/***********************************
 *@Desc TODO
 *@ClassName MultiSourceTrajectoryFusion
 *@Author DLX
 *@Data 2020/8/11 10:13
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MultiSourceTrajectoryFusion {
    private static long time = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        System.out.println("开始执行,当前版本：1.1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String consumerGroupId = paraTool.get("con xcsumer.group.id", UUID.randomUUID().toString());
        System.out.println("consumerGroupId:" + consumerGroupId);

        DataStream<String> mmeStream = KafkaConsumer.createKafkaStream(env, "zz", "MMEConsumerGroup" + consumerGroupId, "DACP_RT_SDTP_S1MME_0", 360, SimpleStringSchema.class);
        DataStream<String> zxStream = KafkaConsumer.createKafkaStream(env, "jc", "ZXConsumerGroup" + consumerGroupId, "ZC_CS_ZX_ZJ_PO01", 50, SimpleStringSchema.class);
        DataStream<String> gsmStream = KafkaConsumer.createKafkaStream(env, "jc", "GSMConsumerGroup" + consumerGroupId, "B_EBU_GSM_NEW", 20, SimpleStringSchema.class);
        DataStream<String> ndStream = KafkaConsumer.createKafkaStream(env, "jc", "NDConsumerGroup" + consumerGroupId, "ZC_CS_ND_ZJ_PO01", 30, SimpleStringSchema.class);
        DataStream<String> ggprsStream = KafkaConsumer.createKafkaStream(env, "jc", "GGPRSConsumerGroup" + consumerGroupId, "topic_ggprs", 180, SimpleStringSchema.class);
        DataStream<String> http4gStream = KafkaConsumer.createKafkaStream(env, "jc", "HTTP4GConsumerGroup" + consumerGroupId, "http_4g_lac", 60, SimpleStringSchema.class);
        DataStream<String> ottStream = KafkaConsumer.createKafkaStream(env, "jc", "OTTConsumerGroup" + consumerGroupId, "TOPIC_OTT", 2, SimpleStringSchema.class);

        SingleOutputStreamOperator<SignalFormat> mmeFlatMapStream = mmeStream.flatMap(new MMEMapFlatFunction()).name("mmeFlatMap").setParallelism(360);
        SingleOutputStreamOperator<SignalFormat> zxFlatMapStream = zxStream.flatMap(new ZXFlatMapFunction()).name("zxFlatMap").setParallelism(50);
        SingleOutputStreamOperator<SignalFormat> gsmFlatMapMStream = gsmStream.flatMap(new GSMFlatMapFunction()).name("gsmFlatMap").setParallelism(20);
        SingleOutputStreamOperator<SignalFormat> ndFlatMapStream = ndStream.flatMap(new NDFlatMapFunction()).name("ndFlatMap").setParallelism(30);
        SingleOutputStreamOperator<SignalFormat> ggprsFlatMapStream = ggprsStream.flatMap(new GGPRSFlatMapFunction()).name("ggprsFlatMap").setParallelism(180);
        SingleOutputStreamOperator<SignalFormat> http4gFlatMap4GStream = http4gStream.flatMap(new HTTP4GFlatMapFunction()).name("http4gFlatMap").setParallelism(60);
        SingleOutputStreamOperator<SignalFormat> ottFlatMapStream = ottStream.flatMap(new OTTFlatMapFunction()).name("ottFlatMap").setParallelism(2);

        WindowedStream<SignalFormat, Tuple, TimeWindow> mmeWindowStream = mmeFlatMapStream.keyBy("phone").timeWindow(Time.of(3, TimeUnit.SECONDS));

        SingleOutputStreamOperator<SignalFormat> mmeReduceStream = mmeWindowStream.reduce(new ReduceFunction<SignalFormat>() {
            @Override
            public SignalFormat reduce(SignalFormat signalFormat1, SignalFormat signalFormat2) throws Exception {
                long a_procedure_start_time = Long.parseLong(signalFormat1.signalTime);
                long b_procedure_start_time = Long.parseLong(signalFormat2.signalTime);
                if (a_procedure_start_time > b_procedure_start_time) {
                    return signalFormat1;
                } else {
                    return signalFormat2;
                }
            }
        }).name("mmeReduce").setParallelism(360);

        DataStream<SignalFormat> fuseStream = mmeReduceStream
                .union(zxFlatMapStream)
                .union(gsmFlatMapMStream)
                .union(ndFlatMapStream)
                .union(ggprsFlatMapStream)
                .union(http4gFlatMap4GStream)
                .union(ottFlatMapStream);

        SingleOutputStreamOperator<SignalFormat> fuseFilterStream = fuseStream.keyBy("phone")
                .filter(new PhoneTimeFilterFunction())
                .startNewChain()
                .name("phoneTimeFilter");

        DataStream<Map<String, BaseStationInfo>> baseInfoStream = env.addSource(new OracleCustomSource())
                .name("OracleCustomSource");

        MapStateDescriptor<String, BaseStationInfo> stateDescriptor = new MapStateDescriptor<>(
                "baseinfo-state",
                String.class,
                BaseStationInfo.class
        );
        BroadcastStream<Map<String, BaseStationInfo>> broadcastState = baseInfoStream.broadcast(stateDescriptor);

        SingleOutputStreamOperator<SignalFormat> fillInfoStream = fuseFilterStream.connect(broadcastState).process(new BroadcastProcessFunction<SignalFormat, Map<String, BaseStationInfo>, SignalFormat>() {
            ReadOnlyBroadcastState<String, BaseStationInfo> mapState;
            BaseStationInfo baseStationInfo;

            @Override
            public void processElement(SignalFormat signalFormat, ReadOnlyContext readOnlyContext, Collector<SignalFormat> collector) throws Exception {
                baseStationInfo = null;
                mapState = readOnlyContext.getBroadcastState(stateDescriptor);
                baseStationInfo = mapState.get(signalFormat.lac_ci);
                if (baseStationInfo != null) {
                    signalFormat.city = baseStationInfo.city_id;
                    signalFormat.county = baseStationInfo.county_id;
                }
                collector.collect(signalFormat);
            }

            @Override
            public void processBroadcastElement(Map<String, BaseStationInfo> stringBaseStationInfoMap, Context context, Collector<SignalFormat> collector) throws Exception {
                BroadcastState<String, BaseStationInfo> MapState = context.getBroadcastState(stateDescriptor);
                MapState.clear();
                MapState.putAll(stringBaseStationInfoMap);
            }
        }).name("fillInfo");

        //统计Multi参数数据量的情况
        SingleOutputStreamOperator<SignalFormat> countStreamNum = fillInfoStream.map(data -> {
            data.signalTime = TimeUtil.stampToDate(data.signalTime, "yyyyMMddHHmmss");
            return data;
        }).returns(TypeInformation.of(SignalFormat.class));

//        countStreamNum.addSink(new MultiMonitor());

        SingleOutputStreamOperator<String> outputStream = fillInfoStream.map(data -> {
            data.signalTime = TimeUtil.stampToDate(data.signalTime, "yyyyMMddHHmmss");
            return data.toString();
        }).returns(Types.STRING);

        KafkaProducer.createKafkaSink(outputStream, "zz", "MULTIPLE_SOURCE_FUSION");
        env.execute("MultiSourceTrajectoryFusion");
    }
}
