package com.asiainfo.dygj.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBTools {
	static{
				try {
					Class.forName("com.mysql.jdbc.Driver");
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	private static Connection conn;
	public static Connection getConn(){
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo", "root", "*963.");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	public static void execute(String sql,Object...objs){
		try {
			PreparedStatement pstat = conn.prepareStatement(sql);
			if(objs!=null){
				for(int i=0;i<objs.length;i++){
					pstat.setObject(i+1, objs[i]);
				}
				
			}
			pstat.execute();
			pstat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static List<Object[]> executeQuery(String sql,Object...objs){
		List<Object[]> list = new ArrayList<Object[]>();
		try {
			PreparedStatement pstat = conn.prepareStatement(sql);
			if(objs!=null){
				for(int i=0;i<objs.length;i++){
					pstat.setObject(i+1, objs[i]);
					
				}
			}
			ResultSet rs = pstat.executeQuery();
			while(rs.next()){
				ResultSetMetaData rsmd = rs.getMetaData();
				int num = rsmd.getColumnCount();
				Object[] obj = new Object[num];
				for(int i=0;i<num;i++){
					obj[i] = rs.getObject(i+1);
					
				}
				list.add(obj);
			}
			rs.close();
			pstat.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	public static void close(Connection conn){
		if(conn!=null){
			try {//�ر�����
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
