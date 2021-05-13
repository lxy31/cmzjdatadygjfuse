package com.asiainfo.dygj.monitor.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName JdbcSink
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/6 18:11
 * @Version 1.0
 **/
public class JdbcSink extends RichSinkFunction<JdbcBean> {
    private Connection conn = null;
    private PreparedStatement updateStmt = null;
    private PreparedStatement insertStmt = null;
    //    private static String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private String URL = "jdbc:mysql://10.76.217.186:3306/mr_info?serverTimezone=UTC";
    private String USER = "mr_info";
    private String PASSWD = "mr_info1q#";
//    private String URL = "jdbc:mysql://192.168.0.113:3306/test?useSSL=false";
//    private String USER = "root";
//    private String PASSWD = "Welcome_1";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(DRIVER_CLASS).newInstance();
        conn = DriverManager.getConnection(URL, USER, PASSWD);
        insertStmt = conn.prepareStatement("INSERT into test (topic,count) values (?,?)");
        updateStmt = conn.prepareStatement("UPDATE test set count = ? where topic = ?");
    }

    @Override
    public void invoke(JdbcBean value, Context context) throws Exception {
        updateStmt.setString(1, value.getTopic());
        updateStmt.setString(2, value.getCount());
        updateStmt.execute();
        if (updateStmt.getUpdateCount() == 0) {
            insertStmt.setString(1, value.getTopic());
            insertStmt.setString(2, value.getCount());
            insertStmt.execute();
        }
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) {
            insertStmt.close();
        }
        if (updateStmt != null) {
            updateStmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
