package com.asiainfo.dygj.sourcefunc;

import com.asiainfo.dygj.bean.BaseStationInfo;
import com.asiainfo.dygj.util.FTPUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/***********************************
 *@Desc TODO
 *@ClassName OracleTestSource
 *@Author DLX
 *@Data 2020/8/11 14:24
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class OracleTestSource extends RichParallelSourceFunction<Map<String, BaseStationInfo>> {
    private Map<Integer,String> ftp_map = new HashMap<Integer,String>();
    private Map<String, BaseStationInfo> resultMap = new HashMap<String, BaseStationInfo>();
    private boolean flag = true;
    private transient FTPClient ftpClient = null;
    private String inpath;
    private String outpath;
    public OracleTestSource() {
        ftp_map.put(0,"20.26.28.80;21;dag;dag;/home/dag/dlx/test;/home/dag/dlx/qs");
        ftp_map.put(1,"20.26.28.80;21;dag;dag;/home/dag/dlx/test1;/home/dag/dlx/qs");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String[] ftp_info = ftp_map.get(getRuntimeContext().getIndexOfThisSubtask()).split(";");
        String host = ftp_info[0];
        int port = Integer.parseInt(ftp_info[1]);
        String userName = ftp_info[2];
        String passWord = ftp_info[3];
        inpath = ftp_info[4];
        outpath = ftp_info[5];
        ftpClient = FTPUtil.loginFTP(host, port, userName, passWord);
    }

    @Override
    public void run(SourceContext<Map<String, BaseStationInfo>> sourceContext) throws Exception {
        while(flag){
            resultMap.clear();
            ftpClient.enterLocalPassiveMode(); // Use passive mode as default
            FTPFile[] ftpFiles = ftpClient.listFiles(inpath);
            for(FTPFile file: ftpFiles){
                String fileName = file.getName();
                InputStream in = ftpClient.retrieveFileStream(inpath+"/"+fileName);//读取目录中的文件
                BufferedReader reader=new BufferedReader(new InputStreamReader(in));
                String str = null;
                while ((str = reader.readLine()) != null){
                    String[] split = str.split("[|]", -1);
                    String lac_ci = split[0];
                    String city_id = split[1];
                    String county_id = split[2];
                    resultMap.put(lac_ci, BaseStationInfo.of(lac_ci,city_id,county_id));
                }
                reader.close();
                in.close();
                ftpClient.completePendingCommand();
            }
            sourceContext.collect(resultMap);
            Thread.sleep(1000*60*30);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

}
