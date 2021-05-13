package com.asiainfo.dygj.monitor;

import java.sql.*;
import java.util.LinkedList;

public class ConnectionPool {
    /*实现连接池在内存中只有一个(单例模式)*/
    private static ConnectionPool pool = new ConnectionPool();
    //用于缓存数据库连接的容器载体
    private final LinkedList<Connection> conns = new LinkedList<Connection>();

    /**
     * 单例模式的构造器，构造连接池时加载了驱动
     */
    private ConnectionPool() {
        try {
            Class.forName(Constant.JDBC_CONNECT_DRIVER_CLASS);
//            Class.forName(Constant.JDBC_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            System.err.println("看看Jar导入了吗？再看看你驱动串写对了吗？");
            e.printStackTrace();
        }
    }

    /**
     * 获取唯一存在的那个连接池实例
     *
     * @return
     */
    public static ConnectionPool getInstance() {
        return pool;
    }


    /**
     * 获取连接，原则：池中有从池中捞，池中没有创建新的
     *
     * @return
     */
    public synchronized Connection getConnection(String flag) {
        Connection conn = null;
        if (conns.size() > 0) {
            conn = conns.removeFirst();
        } else {
            try {
                if (flag.equals("ORACLE")) {
                    conn = DriverManager.getConnection(
                            Constant.JDBC_OWN_ORACLEURL,
                            Constant.ORACLE_NAME,
                            Constant.ORACLE_PASSWORD);
                }
                if (flag.equals("APP")) {
                    conn = DriverManager.getConnection(
                            Constant.JDBC_OTHER_ORACLEURL,
                            Constant.APP_NAME,
                            Constant.APP_PASSWORD);
                }
                if (flag.equals("TEST")) {
                    conn = DriverManager.getConnection(
                            Constant.JDBC_TEST_ORACLEURL,
                            Constant.ORACLE_TEST_NAME,
                            Constant.ORACLE_TEST_PASSWORD);
                }
                if (flag.equals("MYSQL")) {
                    conn = DriverManager.getConnection(
                            Constant.JDBC_DRIVER_CLASSURL,
                            Constant.MR_USERNAME,
                            Constant.MR_PASSWORD);
                }
            } catch (SQLException e) {
                System.err.println("看看你的URL串写对了吗？看看用户名和密码都对吗？");
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 回收连接：池中没满往池中放，池中满了直接关
     *
     * @param conn
     */
    public void revokeConnection(Connection conn) {
        this.revokeConnection(null, null, conn);
    }

    /**
     * 回收连接：池中没满往池中放，池中满了直接关
     *
     * @param stmt
     * @param conn
     */
    public void revokeConnection(Statement stmt, Connection conn) {
        this.revokeConnection(null, stmt, conn);
    }

    /**
     * 回收连接：池中没满往池中放，池中满了直接关
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public void revokeConnection(ResultSet rs, Statement stmt, Connection conn) {
        try {
            // 防止空指针异常的小技巧
            if (rs != null) {
                rs.close();
                if (stmt != null) {
                    stmt.close();
                }
                synchronized (conns) {
                    if (conns.size() < Integer.parseInt(Constant.JDBC_CONNECT_POOL_MAXSIZE)) {
                        conns.addLast(conn);
                    } else {
                        conn.close();
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("回收失败！");
            e.printStackTrace();
        }
    }


    /**
     * 开启指定连接的手动事务
     */
    public void beginTransacation(Connection conn) {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("开启手动事务失败");
            e.printStackTrace();
        }
    }

    /**
     * 提交指定连接的事务
     */
    public void commit(Connection conn) {
        try {
            conn.commit();
            System.out.println("Oralce事务提交成功！");
        } catch (SQLException e) {
            System.err.println("手动提交事务失败！！！");
            e.printStackTrace();
        }
    }

    /**
     * 回滚指定连接的事务
     */
    public void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            System.err.println("手动回滚事务失败");
            e.printStackTrace();
        }
    }
}
