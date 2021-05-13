package com.asiainfo.dygj.sourcefunc;

import com.asiainfo.dygj.bean.BaseStationInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/***********************************
 *@Desc TODO
 *@ClassName OracleCustomSource
 *@Author DLX
 *@Data 2020/8/17 14:07
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class OracleCustomSource extends RichSourceFunction<Map<String, BaseStationInfo>> {
    private transient Connection conn = null;
    private transient PreparedStatement preparedStatement = null;
    private boolean flag = true;
    private Map<String, BaseStationInfo> resultMap = new HashMap<String, BaseStationInfo>();
    private static String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static String URL = "jdbc:oracle:thin:@10.70.98.51:1521:pdb_damp";
    private static String USERNAME = "datastash_prod";
    private static String PASSWORD = "datastash_prod_1Q#";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(DRIVER_CLASS).newInstance();
        System.out.println("1.加载驱动:"+DRIVER_CLASS);
    }

    @Override
    public void run(SourceContext<Map<String, BaseStationInfo>> sourceContext) throws Exception {
        String sql = "SELECT DISTINCT LAC_CI,CITY_ID,COUNTY_ID FROM I_CDM_LACCI";
        System.out.println("2.加载SQL:"+sql);
        while(flag){
            try{
                resultMap.clear();
                conn = DriverManager.getConnection(URL,USERNAME,PASSWORD);
                System.out.println("3.获取连接:"+conn);
                preparedStatement = conn.prepareStatement(sql);
                System.out.println("4.执行查询:"+preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                System.out.print("5.查询成功");
                while (resultSet.next()){
                    String lac_ci = resultSet.getString("LAC_CI");
                    String city_id = resultSet.getString("CITY_ID");
                    String county_id = resultSet.getString("COUNTY_ID");
                    System.out.println("正在组装Map:"+lac_ci+"|"+city_id+"|"+county_id);
                    resultMap.put(lac_ci, BaseStationInfo.of(lac_ci,city_id,county_id));
                }
                sourceContext.collect(resultMap);
            }finally {
                if (preparedStatement != null){
                    preparedStatement.close();
                }
                if (conn != null){
                    conn.close();
                }
                Thread.sleep(1000*60*60*12);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
