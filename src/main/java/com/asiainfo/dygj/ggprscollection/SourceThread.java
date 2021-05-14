package com.asiainfo.dygj.ggprscollection;

import com.asiainfo.dygj.util.FTPUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;


/**
 * @program: cmzjdatadygjfuse
 * @description: 多线程读取FTP上文件
 * @author: Mr.Deng -> Mr.Liu
 * @create: 2021-05-14 16:04
 **/
public class SourceThread implements Runnable {
    private SourceFunction.SourceContext<String> sourceContext;
    private String inpath;
    private String fileName;
    private String fileOutPath;
    private String host;
    private int port;
    private String userName;
    private String passWord;

    public SourceThread(SourceFunction.SourceContext<String> sourceContext, String inpath, String fileName, String fileOutPath, String host, int port, String userName, String passWord) {
        this.sourceContext = sourceContext;
        this.inpath = inpath;
        this.fileName = fileName;
        this.fileOutPath = fileOutPath;
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.passWord = passWord;
    }

    @Override
    public void run() {
        FTPClient ftpClient = null;
        InputStream in = null;
        BufferedReader reader = null;
        try {
            ftpClient = FTPUtil.loginFTP(host, port, userName, passWord);
            in = ftpClient.retrieveFileStream(inpath + "/" + fileName);//读取目录中的文件
            reader = new BufferedReader(new InputStreamReader(in));
            String str = null;
            while ((str = reader.readLine()) != null) {
                sourceContext.collect(str);
            }
            System.out.println("ReadFileName:" + fileName);
            /*close 不可动*/
            in.close();
            reader.close();
            ftpClient.completePendingCommand();
            ftpClient.changeWorkingDirectory(inpath);
            //挪文件
            System.out.println("MoveFileName:" + fileOutPath + "/" + fileName + "\tResult:" + ftpClient.rename(fileName, fileOutPath + "/" + fileName));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                Objects.requireNonNull(ftpClient).logout();// 退出登录
                ftpClient.disconnect();// 断开连接
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}