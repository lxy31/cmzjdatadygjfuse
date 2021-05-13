package com.asiainfo.dygj.monitor.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @ClassName TempSource
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/7 11:01
 * @Version 1.0
 **/
public class TempSource extends RichSourceFunction<Tuple2<String, String>> {
    boolean flag = true;

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        while (flag) {
            for (int i = 0; i < 10; i++) {
                ctx.collect(new Tuple2<>(String.valueOf(i), String.valueOf(i + 1)));
            }
            Thread.sleep(1000 * 60 * 2);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
