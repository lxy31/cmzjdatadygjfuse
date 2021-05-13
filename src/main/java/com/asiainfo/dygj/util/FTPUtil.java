package com.asiainfo.dygj.util;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;


/***********************************
 *@Desc TODO
 *@ClassName Oracle2Ftp
 *@Author DLX
 *@Data 2020/6/8 14:45
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class FTPUtil {

    /**
     * 登陆FTP并获取FTPClient对象
     * @param host     FTP主机地址
     * @param port     FTP端口
     * @param userName 登录用户名
     * @param password 登录密码
     * @return
     */
    public static FTPClient loginFTP(String host, int port, String userName, String password) {
        FTPClient ftpClient = null;
        try {
            ftpClient = new FTPClient();
            // 连接FTP服务器
            ftpClient.connect(host, port);
            // 登陆FTP服务器
            ftpClient.login(userName, password);
            // 中文支持
            ftpClient.setControlEncoding("UTF-8");
            // 设置文件类型为二进制（如果从FTP下载或上传的文件是压缩文件的时候，不进行该设置可能会导致获取的压缩文件解压失败）
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
                System.err.println("连接FTP失败，用户名或密码错误。");
                ftpClient.disconnect();
            } else {
                System.out.println(Thread.currentThread().getName()+"FTP连接成功!");
            }
        } catch (Exception e) {
            System.err.println("登陆FTP失败，请检查FTP相关配置信息是否正确！");
            e.printStackTrace();
        }
        return ftpClient;
    }

    /**
     * 删除单个文件
     *            要删除的文件的文件名
     * @return 单个文件删除成功返回true，否则返回false
     */
    public static boolean deleteFile(File file) {
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                return true;
            } else {
                System.out.println("删除"+file+"失败！");
                return false;
            }
        } else {
            System.out.println("删除单个文件失败,文件不存在！");
            return false;
        }
    }

    /**
     * 上传本地文件到FTP
     * @param localFilePath     要上传的本地文件路径
     * @param ftpFilePath       要保存的FTP文件路径
     */
    public static void uploadFileToFTP(FTPClient ftpClient1, File localFilePath, String ftpFilePath) {
        OutputStream os = null;
        FileInputStream fis = null;
        try {
            os = ftpClient1.storeFileStream(ftpFilePath);
            fis = new FileInputStream(localFilePath);
            int length;
            byte[] bytes = new byte[1024];
            while ((length = fis.read(bytes)) != -1) {
                os.write(bytes, 0, length);
//                System.out.println(new String(bytes));
            }
//            deleteFile(localFilePath);
            System.out.println("FTP文件上传成功！");
        } catch (Exception e) {
            System.err.println("FTP文件上传失败！");
            e.printStackTrace();
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
                if (os != null) {
                    os.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    /**
//     * oracle查询数据到FTP
//     * @param ipAndBs        要上传的数据集合
//     * @param ftpFilePath       要保存的FTP文件路径
//     */
//    public static void oracleToFTP(FTPClient ftpClient1,List<IpAndBs> ipAndBs, String ftpFilePath) {
//        OutputStream os = null;
//        try {
//            os = ftpClient1.storeFileStream(ftpFilePath);
//            for (IpAndBs ib:ipAndBs){
//                os.write(ib.toString().getBytes());
//            }
////            deleteFile(localFilePath);
//            System.out.println("FTP文件上传成功！");
//        } catch (Exception e) {
//            System.err.println("FTP文件上传失败！");
//            e.printStackTrace();
//        } finally {
//            try {
//                if (os != null) {
//                    os.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }


    /**
     * 查询数据到FTP
     * @param strs        要上传的数据集合
     * @param ftpFilePath       要保存的FTP文件路径
     */
    public static void linuxToFTP(FTPClient ftpClient1, List<String> strs, String ftpFilePath) {
        OutputStream os = null;
        try {
            os = ftpClient1.storeFileStream(ftpFilePath);
            for (String str:strs){
                os.write(str.getBytes());
            }
//            deleteFile(localFilePath);
            System.out.println("FTP文件上传成功！");
        } catch (Exception e) {
            System.err.println("FTP文件上传失败！");
            e.printStackTrace();
        } finally {
            try {
                if (os != null) {
                    os.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
