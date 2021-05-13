package com.asiainfo.dygj.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class OracleDBTools {
    private static final int M = 10000;
    private static int count = 0;
    private static long currentTime = 0;
    private static String strSql = "";
    private static final ArrayList<Object[]> list = new ArrayList<>();
    private static final CopyOnWriteArrayList<Object[]> arrayList = new CopyOnWriteArrayList<>(list);
    private static final DBTool dbTool = new DBTool();

    static {
        currentTime = System.currentTimeMillis();
    }

    /**
     * 模拟批量数据出现
     *
     * @param sql  输入oracle增删改查的SQL语句
     * @param objs 输入一条语句所需要的参数
     */
    public static void excute(String sql, String flag, Object... objs) {
        strSql = sql;
        Object[] obj1 = new Object[objs.length];
        int i = 0;
        for (Object obj : objs) {
            obj1[i++] = obj;
        }
        //数量控制
        arrayList.add(obj1);
        if (arrayList.size() == M) {
            //监控专用
            dbTool.updateByPrepareStmtBatch(sql, flag, arrayList);
            arrayList.clear();
        }
    }


    /**
     * @return [java.lang.String, java.lang.String, java.lang.Object...]
     * @Author 刘晓雨
     * @Description //监控500条数据值将一条数据导入数据库， //监控专用
     * @Date 20:22 2021/3/15
     * @Param [sql, flag, objs]
     **/
    public static void excutemonitor(String sql, String flag, Object... objs) {
        strSql = sql;
        Object[] obj1 = new Object[objs.length];
        int i = 0;
        for (Object obj : objs) {
            obj1[i++] = obj;
        }
        //数量控制
        count++;
        arrayList.add(obj1);
        //时间控制
        if (System.currentTimeMillis() > currentTime + 1000 * 60 && count != 0) {
            count = 0;
            currentTime = System.currentTimeMillis();
            dbTool.updateByPrepareStmtBatch(sql, flag, arrayList);
            arrayList.clear();
        }
    }

    /**
     * 清理剩余存在arrayList的数据
     */
    public static void clearData(String flag) {
        if (!arrayList.isEmpty()) {
            System.out.println("\n********开始执行剩余的部分********");
            dbTool.updateByPrepareStmtBatch(strSql, flag.trim(), arrayList);
            arrayList.clear();
        }
    }

    /**
     * @param sql 输入oracle需要的查询的语句
     * @return 返回利用map进行传递key和value值
     */
    public static List<Map<String, Object>> excuteQuery(String sql, String flag) {
        return dbTool.queryBySQL(sql, flag);
    }
}
