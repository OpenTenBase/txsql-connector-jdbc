package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.multiDataSource;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.concurrent.*;

public class TdSqlDemo_MultiDataSourcePre {
    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL1 = "jdbc:tdsql-mysql:direct://9.30.2.89:15024,9.30.2.94:15024/test_2" +
            "?useLocalSessionStates=true" +
            "&useUnicode=true&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro" +
            "&tdsqlDirectMaxSlaveDelaySeconds=0" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=sed";
    private static final String DB_URL2 = "jdbc:tdsql-mysql:direct://9.30.2.89:15024,9.30.2.116:15024/test_2" +
            "?useLocalSessionStates=true" +
            "&useUnicode=true&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=rw" +
            "&tdsqlDirectMaxSlaveDelaySeconds=200" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlLoadBalanceStrategy=sed&autoReconnect=true";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C1";
    private static final DruidDataSource dataSource1 = new DruidDataSource();
    private static final DruidDataSource dataSource2 = new DruidDataSource();

    static {
        try {
            Class.forName(DRIVER_NAME);

            dataSource1.setUrl(DB_URL1);
            dataSource1.setUsername(USERNAME);
            dataSource1.setPassword(PASSWORD);
            dataSource1.setDriverClassName(DRIVER_NAME);
            dataSource1.setInitialSize(10);
            dataSource1.setMaxActive(20);
            dataSource1.setMinIdle(10);
            dataSource1.setValidationQuery("select 1");
            dataSource1.setTimeBetweenEvictionRunsMillis(30000);
            dataSource1.setTestWhileIdle(true);
            dataSource1.setPhyTimeoutMillis(20000);
            dataSource1.setTestOnBorrow(true);
            dataSource1.init();

            dataSource2.setUrl(DB_URL2);
            dataSource2.setUsername(USERNAME);
            dataSource2.setPassword(PASSWORD);
            dataSource2.setDriverClassName(DRIVER_NAME);
            dataSource2.setInitialSize(10);
            dataSource2.setMaxActive(20);
            dataSource2.setMinIdle(10);
            dataSource2.setValidationQuery("select 1");
            dataSource2.setTimeBetweenEvictionRunsMillis(30000);
            dataSource2.setTestWhileIdle(true);
            dataSource2.setPhyTimeoutMillis(20000);
            dataSource2.setTestOnBorrow(true);
            dataSource2.init();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void TestConn() throws InterruptedException {
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                1000,
                1000,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100000),
                new AbortPolicy());
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> System.out.println("Time: " + LocalTime.now() + ", Pool Size: " + executorService.getPoolSize()
                + ", queue Size: " + executorService.getQueue().size()), 0, 1000, TimeUnit.MILLISECONDS);

        while (true) {
            TimeUnit.MILLISECONDS.sleep(4);
            try {
//                executorService.execute(new QueryTask1());
                executorService.execute(new QueryTask2());
            } catch (Exception e) {
                final long cost_err = System.currentTimeMillis();
//                System.out.println("异常结束，时间：" + cost_err);
                e.printStackTrace();
            }
        }
    }




    private static class QueryTask1 implements Runnable {

        @Override
        public void run() {
//            System.out.println("开始执行查询");
            final long cur = System.currentTimeMillis();

            try (Connection conn = dataSource1.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT `id`, `name` FROM t_user limit 1")) {

                int activeCount = dataSource1.getActiveCount();//当前连接数
                int idlecount = dataSource1.getPoolingCount();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                int createcout = dataSource.getCreateTaskCount();
                System.out.println("DruidDataSource——————1 activeCount:" + activeCount);
//                System.out.println("DruidDataSource IdleCount:" + idlecount);
//                System.out.println("DruidDataSource createCount:"+createcout);
                while (rs.next()) {
                    System.out.println("ID: " + rs.getLong(1));
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

    private static class QueryTask2 implements Runnable {

        @Override
        public void run() {
//            System.out.println("开始执行查询");
            final long cur = System.currentTimeMillis();

            try (Connection conn = dataSource2.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT `id`, `name` FROM t_user limit 1")) {

                int activeCount = dataSource2.getActiveCount();//当前连接数
                int idlecount = dataSource2.getPoolingCount();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                int createcout = dataSource.getCreateTaskCount();
                System.out.println("DruidDataSource————————2 activeCount:" + activeCount);
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
}
