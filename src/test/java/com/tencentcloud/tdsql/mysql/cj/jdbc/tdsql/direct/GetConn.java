package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.alibaba.druid.pool.DruidDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.base.BaseTest;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GetConn extends BaseTest {
    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.1.207:15006,9.30.1.231:15006/test?useLocalSessionStates=true" +
            "&useUnicode=true&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro" +
            "&tdsqlDirectMaxSlaveDelaySeconds=50" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=Sed";
    private static final String USERNAME = "tdsqlsys_normal";
    private static final String PASSWORD = "5R77aqf9kSk8HnN%R";
    private static final DruidDataSource dataSource = new DruidDataSource();

    static {
        try {
            Class.forName(DRIVER_NAME);

            dataSource.setUrl(DB_URL);
            dataSource.setUsername(USERNAME);
            dataSource.setPassword(PASSWORD);
            dataSource.setDriverClassName(DRIVER_NAME);
            dataSource.setInitialSize(10);
            dataSource.setMaxActive(20);
            dataSource.setMinIdle(10);
            dataSource.setValidationQuery("select 1");
            dataSource.setTimeBetweenEvictionRunsMillis(10000);
            dataSource.setTestWhileIdle(true);
            dataSource.setPhyTimeoutMillis(20000);
            dataSource.init();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void TestConn(){
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                1000,
                1000,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100000),
                new AbortPolicy());

        while (true) {
            try {
                executorService.execute(new QueryTask());
            } catch (Exception e) {
                final long cost_err = System.currentTimeMillis();
//                System.out.println("异常结束，时间：" + cost_err);
                e.printStackTrace();
            }
        }
    }
    private static class QueryTask implements Runnable {

        @Override
        public void run() {
//            System.out.println("开始执行查询");
            final long cur = System.currentTimeMillis();

            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT `id`, `name` FROM t_user limit 1")) {

                int activeCount = dataSource.getActiveCount();//当前连接数
                int idlecount = dataSource.getPoolingCount();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                int createcout = dataSource.getCreateTaskCount();
                System.out.println("DruidDataSource activeCount:" + activeCount);
//                System.out.println("DruidDataSource IdleCount:" + idlecount);
//                System.out.println("DruidDataSource createCount:"+createcout);
                while (rs.next()) {
//                    System.out.println("ID: " + rs.getLong(1));
                }
                final long cost = System.currentTimeMillis();
//                System.out.println("正常结束，耗时：" + cost);
            } catch (Exception e) {
                final long cost_err = System.currentTimeMillis();
//                System.out.println("异常结束，时间：" + cost_err);
                e.printStackTrace();
            }

            final long cost = System.currentTimeMillis();
//            System.out.println("正常结束，耗时：" + cost);
        }
    }

    private static class AbortPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
//            System.out.println("Task " + r.toString() + " rejected from " + e.toString());
        }
    }
}
