package com.asiainfo.dygj.monitor.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkJdbcTest
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/6 18:05
 * @Version 1.0
 **/
public class FlinkJdbcTest {
    public static void main(String[] args) throws Exception {
        System.out.println("开始");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, String>> source = env.addSource(new TempSource());
        SingleOutputStreamOperator<JdbcBean> jdbcStream = source.map(data -> new JdbcBean(data.f0, data.f1)).returns(TypeInformation.of(JdbcBean.class));
        jdbcStream.print("JdbcBean");
        jdbcStream.addSink(new JdbcSink());
        env.execute("TestJdbc");
    }
}
