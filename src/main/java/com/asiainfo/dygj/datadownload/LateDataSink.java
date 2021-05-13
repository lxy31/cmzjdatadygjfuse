package com.asiainfo.dygj.datadownload;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.HdfsUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

/***********************************
 *@Desc TODO
 *@ClassName LateDataSink
 *@Author DLX
 *@Data 2020/9/11 11:07
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class LateDataSink extends RichSinkFunction<SignalFormat> {
    private String path;
    private List<String> lines = new ArrayList<String>();
    private long firstDataTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取全局参数
        ParameterTool params = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        path = params.get("save.hdfs.path", "hdfs://nsfed/ns2/jc_zz_swrz/dygjoutput/MULTIPLE_SOURCE_FUSION_test/");
        firstDataTime = System.currentTimeMillis();
//        path = "hdfs://nsfed/ns2/jc_zz_swrz/dygjoutput/MULTIPLE_SOURCE_FUSION_test/";
    }

    @Override
    public void invoke(SignalFormat value, Context context) throws Exception {
        long nowDataTime = System.currentTimeMillis();
        lines.add(value.toString());
        if (lines.size()>1300000 || nowDataTime-firstDataTime>(1000*60*30)){
            String lataDataOutPutPath = path+"latedata/"+value.signalTime.replaceAll("-","").replace(" ","")+"_"+nowDataTime+"_"+getRuntimeContext().getIndexOfThisSubtask()+".txt";
            HdfsUtil.putLineToHDFS(lines,lataDataOutPutPath);
            firstDataTime = nowDataTime;
            lines.clear();
        }
    }
}
