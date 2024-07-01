package tdsql.loadbalance;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;

public class MultiLbAndDirectTest extends BaseTest {

    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL1 = "jdbc:tdsql-mysql:direct:" +
            "//" + PROXY_1 + "," + PROXY_2 + "/test" +
            "?logger=Slf4JLogger" +
            "&tdsqlDirectReadWriteMode=ro" +
            "&autoReconnect=true";
    private static final String DB_URL2 = "jdbc:tdsql-mysql:loadbalance:" +
            "//" + PROXY_1 + "," + PROXY_2 + "/test" +
            "?tdsqlLoadBalanceStrategy=sed" +
            "&logger=Slf4JLogger" +
            "&tdsqlLoadBalanceWeightFactor=2,1" +
            "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
            "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
            "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
            "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
            "&autoReconnect=true";

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
        ds.setUsername(USER);
        ds.setPassword(PASS);
        ds.setDriverClassName(DRIVER_NAME);
        ds.setInitialSize(10);
        ds.setMaxActive(10);
        ds.setMinIdle(10);
        ds.setValidationQuery("select 1");
        ds.setTimeBetweenEvictionRunsMillis(30000);
        ds.setTestWhileIdle(true);
        ds.setPhyTimeoutMillis(10000);
        ds.setTestOnBorrow(true);
        ds.init();
    }

    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
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
                () -> System.out.println(
                        "Time: " + LocalTime.now() + ", Active Size: " + executorService.getActiveCount()
                                + ", queue Size: " + executorService.getQueue().size() + ", ds1 Active: "
                                + ds1.getActiveCount()
                                + ", ds2 Active: " + ds2.getActiveCount()), 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void TestConn() throws Exception {
        initDataSource(ds1);
        initDataSource(ds2);
        initMonitor();

        try {
            long startTime = System.currentTimeMillis();
            while (true) {
                TimeUnit.MILLISECONDS.sleep(100);
                executorService.execute(new QueryTask());
                long endTime=System.currentTimeMillis(); //获取结束时间
                if (endTime - startTime > (1000 * 60 * 2)) {
                    break;
                }
            }
        } finally {
            executorService.shutdownNow();
            ds1.close();
            ds2.close();
        }

    }

    private static class QueryTask implements Runnable {

        @Override
        public void run() {

            try (Connection conn = ds1.getConnection();
                    Statement stmt = conn.createStatement();
                    Connection conn1 = ds2.getConnection();
                    Statement stmt1 = conn1.createStatement();
                    ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM t_user");
                    ResultSet rs2 = stmt1.executeQuery("SELECT COUNT(*) FROM t_user")) {

                if (rs1.next()) {
                    System.out.println("1 query t_user_counts: " + rs1.getLong(1));
                }
                if (rs2.next()) {
                    System.out.println("2 query t_user_counts: " + rs2.getLong(1));
                }

                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Date d = new Date();
                System.out.println("正常结束时间" + d);

            } catch (SQLException e) {
                Date d = new Date();
                System.out.println("异常结束时间" + d);
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
