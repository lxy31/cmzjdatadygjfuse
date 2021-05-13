package com.asiainfo.dygj.monitor;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DBTool {
    // 连接
    private Connection conn;
    // 处理对象
    private Statement stmt;
    // 编译预处理对象
    private PreparedStatement pstmt;
    // 结果集
    private ResultSet rs;

    //用于控制批处理的批量阈值
    private int batchCount;

    //将list提取防止查询数量过导致内存溢出
    private final List<Map<String, Object>> list = new ArrayList<>();

    /**
     * 编译预处理方式执行查询(SELECT)DML操作的SQL语句
     *
     * @param sql    待执行的查询(SELECT)DML操作的SQL语句
     * @param values 向编译预处理方式的SQL语句填充的数据的集合
     * @return 查询结果
     */
    public List<Map<String, Object>> queryByPrepared(String sql, String flag, Object... values) {
        List<Map<String, Object>> list = null;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            pstmt = conn.prepareStatement(sql);
            for (int index = 0; index < values.length; index++) {
                pstmt.setObject(index + 1, values[index]);
            }
            rs = pstmt.executeQuery();
            list = this.getListResults(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, pstmt, conn);
        }
        return list;
    }


    /**
     * 执行查询(SELECT)DML操作的SQL语句
     *
     * @param sql 待执行的查询(SELECT)DML操作的SQL语句
     * @return 查询结果
     */
    public List<Map<String, Object>> queryBySQL(String sql, String flag) {
        // 查询后的结果集容器化处理
        List<Map<String, Object>> list = null;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pstmt.setFetchSize(1000);
            // 执行查询
            rs = pstmt.executeQuery();
            list = this.getListResults(rs);
        } catch (SQLException e) {
            System.err.println("看看你的Select的SQL语句写的对吗？");
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, pstmt, conn);
        }
        return list;
    }

    /**
     * 执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sql 待执行的增(INSERT)删(DELETE)改(UPDATE)SQL语句
     * @return 影响记录总数
     */
    public int updateBySQL(String sql, String flag) {
        int count = 0;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            stmt = conn.createStatement();
            // 设置自动提交事务为false,即设置为手动提交事务
            ConnectionPool.getInstance().beginTransacation(conn);
            count = stmt.executeUpdate(sql);
            // 手动提交事务
            ConnectionPool.getInstance().commit(conn);
        } catch (SQLException e) {
            // 手动回滚事务
            ConnectionPool.getInstance().rollback(conn);
            System.err.println("看看你的Insert或Update或Delete的SQL语句写的对吗？");
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, stmt, conn);
        }
        return count;
    }


    /**
     * 编译预处理模式执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sql    待执行的编译预处理模式的增(INSERT)删(DELETE)改(UPDATE)SQL语句
     * @param values 执行预处理SQL时需要填充的数据
     * @return 影响记录总数
     */
    public int updateByPrepared(String sql, String flag, Object... values) {
        int count = 0;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            pstmt = conn.prepareStatement(sql);
            for (int index = 0; index < values.length; index++) {
                pstmt.setObject(index + 1, values[index]);
            }
            // 设置自动提交事务为false,即设置为手动提交事务
            ConnectionPool.getInstance().beginTransacation(conn);
            count = pstmt.executeUpdate();
            // 手动提交事务
            ConnectionPool.getInstance().commit(conn);
        } catch (SQLException e) {
            // 手动回滚事务
            ConnectionPool.getInstance().rollback(conn);
            System.err.println("看看你的Insert或Update或Delete的SQL语句写的对吗？");
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, pstmt, conn);
        }
        return count;
    }


    /**
     * 批处理模式执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sqls 待执行的批处理模式的增(INSERT)删(DELETE)改(UPDATE)SQL语句集合
     * @return 影响记录总数集
     */
    public int[] updateByStmtBatch(String flag, String... sqls) {
        List<String> sqlList = new ArrayList<>();
        if (sqls != null) {
            Collections.addAll(sqlList, sqls);
        }
        return this.updateByStmtBatch(sqlList, flag);
    }

    /**
     * 批处理模式执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sqls 待执行的批处理模式的增(INSERT)删(DELETE)改(UPDATE)SQL语句集合
     * @return 影响记录总数集
     */
    public int[] updateByStmtBatch(List<String> sqls, String flag) {
        int[] counts = null;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            stmt = conn.createStatement();
            for (String sql : sqls) {
                stmt.addBatch(sql);
                //为批处理做的优化处理
                batchCount++;
                if (batchCount >= Integer.parseInt(Constant.JDBC_EXEC_BATCH_SIZE)) {
                    // 设置自动提交事务为false,即设置为手动提交事务
                    ConnectionPool.getInstance().beginTransacation(conn);
                    counts = stmt.executeBatch();
                    // 手动提交事务
                    ConnectionPool.getInstance().commit(conn);
                    batchCount = 0;
                    stmt.clearBatch();
                }
            }
            // 设置自动提交事务为false,即设置为手动提交事务
            ConnectionPool.getInstance().beginTransacation(conn);
            counts = stmt.executeBatch();
            // 手动提交事务
            ConnectionPool.getInstance().commit(conn);
            batchCount = 0;
            stmt.clearBatch();
        } catch (SQLException e) {
            // 手动回滚事务
            ConnectionPool.getInstance().rollback(conn);
            System.err.println("看看你的Insert或Update或Delete的SQL语句写的对吗？");
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, stmt, conn);
        }
        return counts;
    }


    /**
     * 编译预处理模式的批处理模式执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sql       待执行的编译预处理模式的批处理模式的增(INSERT)删(DELETE)改(UPDATE)SQL语句集合
     * @param //isclose 执行批量的SQL语句后是否关闭连接
     * @param values    执行编译预处理模式的批处理SQL时需要填充的数据
     * @return 影响记录总数集
     */
    public int[] updateByPrepareStmtBatch(String sql, String flag, Object[]... values) {
        List<Object[]> list = new ArrayList<Object[]>();
        CopyOnWriteArrayList<Object[]> valueList = new CopyOnWriteArrayList<>(list);
        valueList.addAll(Arrays.asList(values));
        return this.updateByPrepareStmtBatch(sql, flag, valueList);
    }

    /**
     * 编译预处理模式的批处理模式执行增(INSERT)删(DELETE)改(UPDATE)DML的SQL语句
     *
     * @param sql    待执行的编译预处理模式的批处理模式的增(INSERT)删(DELETE)改(UPDATE)SQL语句集合
     * @param values 执行编译预处理模式的批处理SQL时需要填充的数据
     * @return 影响记录总数集
     */
    public int[] updateByPrepareStmtBatch(String sql, String flag, CopyOnWriteArrayList<Object[]> values) {
        int[] counts = null;
        try {
            conn = ConnectionPool.getInstance().getConnection(flag);
            pstmt = conn.prepareStatement(sql);
            for (Object[] value : values) {
                for (int index = 0; index < value.length; index++) {
                    pstmt.setObject(index + 1, value[index]);
                }
                pstmt.addBatch();
                //为批处理做的优化处理,这个过程A应该封装
                batchCount++;
                if (batchCount >= Integer.parseInt(Constant.JDBC_EXEC_BATCH_SIZE)) {
                    // 设置自动提交事务为false,即设置为手动提交事务
                    ConnectionPool.getInstance().beginTransacation(conn);
                    counts = pstmt.executeBatch();
                    // 手动提交事务
                    ConnectionPool.getInstance().commit(conn);
                    batchCount = 0;
                    pstmt.clearBatch();
                }
            }
            // 设置自动提交事务为false,即设置为手动提交事务
            ConnectionPool.getInstance().beginTransacation(conn);
            counts = pstmt.executeBatch();
            // 手动提交事务
            ConnectionPool.getInstance().commit(conn);
            batchCount = 0;
            pstmt.clearBatch();
        } catch (SQLException e) {
            // 手动回滚事务
            ConnectionPool.getInstance().rollback(conn);
            System.err.println("看看你的Insert或Update或Delete的SQL语句写的对吗？");
            e.printStackTrace();
        } catch (Exception ex) {
            System.out.println("数据提交数据库Conn和Sql是否获取到：conn -->" + conn + ",sql --> " + sql);
        } finally {
            ConnectionPool.getInstance().revokeConnection(rs, pstmt, conn);
        }
        return counts;
    }

    /**
     * 将ResultSet对象转化为List<Map<String,Object>>格式的对象
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private List<Map<String, Object>> getListResults(ResultSet rs) throws SQLException {
        list.clear();
        //测试读取数据时间
        int count = 0;
        long startTime = System.currentTimeMillis();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int index = 1; index <= rs.getMetaData().getColumnCount(); index++) {
                // 获取一个字段的标签名
                String key = rs.getMetaData().getColumnLabel(index);
                // 获取一个字段的value值
                Object value = rs.getObject(index);
                // 得到一个字段存一个进入map
                map.put(key, value);
                count++;
            }
            list.add(map);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("-=-=-=-=-=-=-=-=-=-=-=读取数据库耗时：" + (endTime - startTime) + ",读取数据个数：" + count + "个-=-=-=-=-=-=-=-=-=-=-=");
        return list;
    }
}
