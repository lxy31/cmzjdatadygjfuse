package com.asiainfo.dygj.ggprscollection;

import com.asiainfo.dygj.util.FTPUtil;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/***********************************
 *@Desc TODO
 *@ClassName OracleTestSource
 *@Author DLX
 *@Data 2020/8/11 14:24
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//DR_GGPRS_SX_0_20201016_44933000320201016135805697.D.dat
public class GGPRSFileSource extends RichParallelSourceFunction<String> {
    private Map<Integer, String> ftp_map = new HashMap<Integer, String>();
    private boolean flag = true;
    private transient FTPClient ftpClient = null;
    private String inpath;
    private String outpath;
    private int slotNum;
    private ThreadPoolExecutor es2;
    private String host;
    private int port;
    private String userName;
    private String passWord;

    public GGPRSFileSource() {
        ftp_map.put(0,"10.78.142.132;21;jf;GPrr@Jf639;/srv/BigData/flume1/jf/jf1;/srv/BigData/flume1/jf/shsnc/ggprs");
        ftp_map.put(1,"10.78.142.132;21;jf;GPrr@Jf639;/srv/BigData/flume1/jf/jf2;/srv/BigData/flume1/jf/shsnc/ggprs");
        ftp_map.put(2,"10.78.142.132;21;jf;GPrr@Jf639;/srv/BigData/flume1/jf/jf3;/srv/BigData/flume1/jf/shsnc/ggprs");
        ftp_map.put(3,"10.78.142.132;21;jf;GPrr@Jf639;/srv/BigData/flume1/jf/jf4;/srv/BigData/flume1/jf/shsnc/ggprs");
        ftp_map.put(4,"10.78.142.132;21;jf;GPrr@Jf639;/srv/BigData/flume1/jf/jf5;/srv/BigData/flume1/jf/shsnc/ggprs");
        ftp_map.put(5,"10.78.142.154;21;jf;GPrr@Jf639;/srv/BigData/hadoop/data1/jf/jf6;/srv/BigData/hadoop/data1/jf/shsnc/ggprs");
        ftp_map.put(6,"10.78.142.154;21;jf;GPrr@Jf639;/srv/BigData/hadoop/data1/jf/jf7;/srv/BigData/hadoop/data1/jf/shsnc/ggprs");
        ftp_map.put(7,"10.78.142.154;21;jf;GPrr@Jf639;/srv/BigData/hadoop/data1/jf/jf8;/srv/BigData/hadoop/data1/jf/shsnc/ggprs");
        ftp_map.put(8,"10.78.142.154;21;jf;GPrr@Jf639;/srv/BigData/hadoop/data1/jf/jf9;/srv/BigData/hadoop/data1/jf/shsnc/ggprs");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        es2 = new ThreadPoolExecutor(4,4,1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    }

    public void openFtp() throws Exception {
        slotNum = getRuntimeContext().getIndexOfThisSubtask();
        String[] ftp_info = ftp_map.get(slotNum).split(";");
        host = ftp_info[0];
        port = Integer.parseInt(ftp_info[1]);
        userName = ftp_info[2];
        passWord = ftp_info[3];
        inpath = ftp_info[4];
        outpath = ftp_info[5];
        System.out.println("Login Info:\tSlotNum:"+getRuntimeContext().getIndexOfThisSubtask() + "\tHostAndPort:" + host +":"+ port + "\tUserName:"+userName + "\tPassWord:******" + "\tLoadDataPath:"+inpath);
        ftpClient = FTPUtil.loginFTP(host, port, userName, passWord);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //加载全局参数
//        ParameterTool paraTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//        String ftpType = paraTool.get("ftp.type.id", "0");
//        System.out.println("ftp.type.id:" + ftpType);

        while (flag) {
            openFtp();
            String fileOutPath = outpath + "/" + TimeUtil.timeMillisToFilePath(System.currentTimeMillis() + "");
            ftpClient.enterLocalPassiveMode(); // Use passive mode as default
            FTPFile[] ftpFiles = ftpClient.listFiles(inpath);
            if (!ftpClient.changeWorkingDirectory(fileOutPath)) {//若路径未存在则创建路径
                if (!ftpClient.makeDirectory(fileOutPath)) {//若路径创建失败则不再继续处理
                    System.out.println("create dir fail --> " + fileOutPath);
                }
            }
            for (FTPFile file : ftpFiles) {
                String fileName = file.getName();
                if (fileName.startsWith("DR_GGPRS")) {
                    es2.execute(new SourceThread(sourceContext,inpath,fileName,fileOutPath,host, port, userName, passWord));
                }
            }
            ftpClient.logout();// 退出登录
            ftpClient.disconnect();// 断开连接
            System.out.println("休眠开始！！！！！");
            Thread.sleep(1000 * 60);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

}
