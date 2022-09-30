package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.multiDataSource;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TdSqlDemo_MultiDataSource {

    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL1 = "jdbc:tdsql-mysql:direct://9.30.2.89:15024,9.30.2.94:15024/test_2"
            + "?tdsqlDirectReadWriteMode=ro"
            + "&tdsqlDirectMaxSlaveDelaySeconds=0"
            + "&tdsqlDirectTopoRefreshIntervalMillis=1000"
            + "&tdsqlLoadBalanceStrategy=sed";

    //    private static final String DB_URL1 = "jdbc:tdsql-mysql:direct://9.30.1.207:15006/test?useLocalSessionStates=true" +
    //            "&useUnicode=true&characterEncoding=utf-8" +
    //            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro" +
    //            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
    //            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
    //            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=sed";

    private static final String DB_URL2 = "jdbc:tdsql-mysql:direct://9.30.2.116:15024,9.30.2.250:15024/test_2"
            + "?tdsqlDirectReadWriteMode=rw"
            + "&tdsqlDirectMaxSlaveDelaySeconds=200"
            + "&tdsqlDirectTopoRefreshIntervalMillis=1000"
            + "&tdsqlLoadBalanceStrategy=sed";

    //    private static final String DB_URL2 = "jdbc:tdsql-mysql:direct://9.30.1.207:15006/test_2?useLocalSessionStates=true" +
    //            "&useUnicode=true&characterEncoding=utf-8" +
    //            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro" +
    //            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
    //            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
    //            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=sed";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C1";
    private static final DruidDataSource ds1 = new DruidDataSource();
    private static final DruidDataSource ds2 = new DruidDataSource();
    private ThreadPoolExecutor executorService;

    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void initDataSource(DruidDataSource ds) throws SQLException {
        if (ds == ds1) {
            ds.setUrl(DB_URL1);
        } else {
            ds.setUrl(DB_URL2);
        }
        ds.setUsername(USERNAME);
        ds.setPassword(PASSWORD);
        ds.setDriverClassName(DRIVER_NAME);
        ds.setInitialSize(10);
        ds.setMaxActive(20);
        ds.setMinIdle(10);
        ds.setValidationQuery("select 1");
        ds.setTimeBetweenEvictionRunsMillis(30000);
        ds.setTestWhileIdle(true);
        ds.setPhyTimeoutMillis(10000);
        ds.setTestOnBorrow(true);
        ds.init();
    }

    private void initMonitor() {
        executorService = new ThreadPoolExecutor(
                1000,
                1000,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new AbortPolicy());
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(
                () -> System.out.println("Time: " + LocalTime.now() + ", Active Size: " + executorService.getActiveCount()
                        + ", queue Size: " + executorService.getQueue().size() + ", ds1 Active: " + ds1.getActiveCount()
                        + ", ds2 Active: " + ds2.getActiveCount()), 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void TestConn() throws Exception {
        initDataSource(ds1);
        initDataSource(ds2);
        initMonitor();

        while (true) {
            TimeUnit.MILLISECONDS.sleep(4);
            try {
                executorService.execute(new QueryTask(ds1));
                executorService.execute(new QueryTask(ds2));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static class QueryTask implements Runnable {

        private final DruidDataSource ds;

        public QueryTask(DruidDataSource ds) {
            this.ds = ds;
        }

        @Override
        public void run() {
            try (Connection conn = ds.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT `id`, `name` FROM t_user limit 1")) {
                while (rs.next()) {
                    System.out.println(ds.getName() + ", ID: " + rs.getLong(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class AbortPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            System.out.println("Task " + r.toString() + " rejected from " + e.toString());
        }
    }
}
