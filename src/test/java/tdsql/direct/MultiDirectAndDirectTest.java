package tdsql.direct;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiDirectAndDirectTest {

    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.0.250:15023,9.30.2.116:15023/QT4S" +
            "?useLocalSessionStates=true" +
            "&useUnicode=true" +
            "&logger=Slf4JLogger" +
            "&characterEncoding=gbk&serverTimezone=Asia/Shanghai" +
            "&tdsqlDirectReadWriteMode=rw" +
            "&tdsqlDirectMaxSlaveDelaySeconds=0" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500" +
            "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1" +
            "&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true" +
            "&tdsqlLoadBalanceStrategy=lc";
    private static final String DB_URL1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15023,9.30.2.116:15023/test" +
            "?useLocalSessionStates=true&useUnicode=true" +
            "&logger=Slf4JLogger" +
            "&characterEncoding=utf-8&serverTimezone=Asia/Shanghai" +
            "&tdsqlDirectReadWriteMode=ro" +
            "&tdsqlDirectMaxSlaveDelaySeconds=100" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500" +
            "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1" +
            "&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=false" +
            "&tdsqlLoadBalanceStrategy=lc";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";
    private static final DruidDataSource dataSource = new DruidDataSource();
    private static final DruidDataSource dataSource1 = new DruidDataSource();

    static {
        try {
            Class.forName(DRIVER_NAME);

            dataSource.setUrl(DB_URL);
            dataSource.setUsername(USERNAME);
            dataSource.setPassword(PASSWORD);
            dataSource.setDriverClassName(DRIVER_NAME);
            dataSource.setInitialSize(10);
            dataSource.setMaxActive(10);
            dataSource.setMinIdle(10);
            dataSource.setValidationQuery("select 1");
            dataSource.setTimeBetweenEvictionRunsMillis(10000);
            dataSource.setTestWhileIdle(true);
            //dataSource.setPhyTimeoutMillis(20000);
            dataSource.init();
            dataSource1.setUrl(DB_URL1);
            dataSource1.setUsername(USERNAME);
            dataSource1.setPassword(PASSWORD);
            dataSource1.setDriverClassName(DRIVER_NAME);
            dataSource1.setInitialSize(10);
            dataSource1.setMaxActive(10);
            dataSource1.setMinIdle(10);
            dataSource1.setValidationQuery("select 1");
            dataSource1.setTimeBetweenEvictionRunsMillis(10000);
            dataSource1.setTestWhileIdle(true);
            //dataSource1.setPhyTimeoutMillis(20000);
            dataSource1.init();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                100,
                100,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new AbortPolicy());

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(
                () -> System.out.println(
                        "Time: " + LocalTime.now() + ", Active Size: " + executorService.getActiveCount()
                                + ", Pool size: " + executorService.getPoolSize() + ", Task count: "
                                + executorService.getTaskCount()
                                + ", queue Size: " + executorService.getQueue().size() + ", ds1 Active: "
                                + dataSource.getActiveCount() + ", ds1 create: " + dataSource.getCreateCount() + ", ds1 connect: "
                                + dataSource.getConnectCount()
                                + ", ds2 Active: " + dataSource1.getActiveCount() + ", ds2 create: " + dataSource1.getCreateCount() + ", ds2 connect: "
                                + dataSource1.getConnectCount()), 0, 1000, TimeUnit.MILLISECONDS);

        while (true) {
            try {
                executorService.execute(new QueryTask());
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (Exception e) {
                final long cost_err = System.currentTimeMillis();
                System.out.println("异常结束，时间：" + cost_err);
                e.printStackTrace();
            }
        }
    }

    private static class QueryTask implements Runnable {

        @Override
        public void run() {
            //System.out.println("开始执行查询-1---");
            final long cur = System.currentTimeMillis();

            try (Connection conn = dataSource.getConnection();
                    Connection conn1 = dataSource1.getConnection();
                    Statement stmt = conn.createStatement();
                    Statement stmt1 = conn1.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT `id`, `name` FROM t_user limit 1");
                    ResultSet rs1 = stmt1.executeQuery("SELECT `id`, `name` FROM t_user limit 1")) {

                int activeCount = dataSource.getActiveCount();//当前连接数
                int idlecount = dataSource.getPoolingCount();
                int activeCount1 = dataSource1.getActiveCount();
                int idlecount1 = dataSource1.getPoolingCount();
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //int createcout = dataSource.getCreateTaskCount();
                //System.out.println("DruidDataSource-1- activeCount:" + activeCount);
                //System.out.println("DruidDataSource-2 activeCount:" + activeCount1);
                //System.out.println("DruidDataSource createCount:"+createcout);
                while (rs.next()) {
                    //System.out.println("ID-1: " + rs.getLong(1));
                    TimeUnit.MILLISECONDS.sleep(10);
                }

                while (rs1.next()) {
                    //System.out.println("ID-2-: " + rs1.getString(2));
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                //System.out.println("正常结");
            } catch (Exception e) {
                Date d = new Date();
                System.out.println("异常结束时间" + d);
                e.printStackTrace();
            }
        }
    }

    private static class AbortPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            //System.out.println("Task " + r.toString() + " rejected from " + e.toString());
        }
    }
}
