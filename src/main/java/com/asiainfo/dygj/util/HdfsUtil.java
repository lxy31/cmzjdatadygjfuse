package com.asiainfo.dygj.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.util.List;


/***********************************
 *@Desc TODO
 *@ClassName HdfsUtil
 *@Author DLX
 *@Data 2020/7/20 15:36
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
//com.asiainfo.file.util.HdfsUtil
public class HdfsUtil {
    /**
     * 拉取文件到HDFS
     * @param inStream SFtp通过get方式得到的数据流
     * @param putFileName 文件下载存放路径
     * */
    public static void putFileToHDFS(InputStream inStream,String putFileName) throws Exception{
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        // 2 创建输入流
//        FileInputStream inStream = new FileInputStream(new File("/app/mro/dlx/cmzj-data-mme-compress-1.0-SNAPSHOT.jar"));;
        // 3 获取输出路径
        Path writePath = new Path(putFileName);
        // 4 创建输出流
        FSDataOutputStream outStream = fs.create(writePath);
        // 5 流对接
        try{
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }
    public static void putLineToHDFS(List<String> lines, String putFileName) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path writePath = new Path(putFileName);
        // 4 创建输出流
        FSDataOutputStream outStream = fs.create(writePath);
        // 5 流对接
        try{
            for (String line: lines) {
                outStream.write(line.getBytes("UTF-8"));
                outStream.write("\n".getBytes("UTF-8"));
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(outStream);
        }
    }
//    public static void putFileToHDFSTest(String putFileName) throws Exception{
//        // 1 创建配置信息对象
//        Configuration configuration = new Configuration();
////        FileSystem fs = FileSystem.get(
////                new URI("hdfs://nsfed"),
////                configuration,
////                "jc_zz_mro");
//        FileSystem fs = FileSystem.get(configuration);
//        // 2 创建输入流
//        FileInputStream inStream = new FileInputStream(new File("/app/mro/dlx/cmzj-data-mme-compress-1.0-SNAPSHOT.jar"));
//        // 3 获取输出路径
//        Path writePath = new Path(putFileName);
//        // 4 创建输出流
//        FSDataOutputStream outStream = fs.create(writePath);
//        // 5 流对接
//        try{
//            IOUtils.copyBytes(inStream, outStream, 4096, false);
//        }catch(Exception e){
//            e.printStackTrace();
//        }finally{
//            IOUtils.closeStream(inStream);
//            IOUtils.closeStream(outStream);
//        }
//    }

//    public static void main(String[] args) throws Exception {
//        HdfsUtil.putFileToHDFSTest("hdfs://nsfed/ns2/jc_zz_mro/mme_data/test1");
//    }
}
