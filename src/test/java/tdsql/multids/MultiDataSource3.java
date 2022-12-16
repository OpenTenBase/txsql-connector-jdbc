package tdsql.multids;

import com.alibaba.druid.pool.DruidDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class MultiDataSource3 {

    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL_1 = "jdbc:tdsql-mysql:direct://9.30.2.116:15012/test" +
            "?useLocalSessionStates=true" +
            "&useUnicode=true" +
            "&characterEncoding=gbk&serverTimezone=Asia/Shanghai" +
            "&tdsqlDirectReadWriteMode=rw" +
            "&tdsqlDirectMaxSlaveDelaySeconds=0" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500" +
            "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1" +
            "&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true" +
            "&tdsqlLoadBalanceStrategy=lc" +
            "&logger=Slf4JLogger" +
            "&autoReconnect=true&socketTimeout=1000";
    private static final String DB_URL_2 = "jdbc:tdsql-mysql:direct://9.30.2.116:15012/test" +
            "?useLocalSessionStates=true" +
            "&useUnicode=true&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai" +
            "&tdsqlDirectReadWriteMode=ro" +
            "&tdsqlDirectMaxSlaveDelaySeconds=100" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500" +
            "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1" +
            "&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=false" +
            "&tdsqlLoadBalanceStrategy=lc" +
            "&logger=Slf4JLogger" +
            "&autoReconnect=true&socketTimeout=1000";
    private static final String DB_URL_3 = "jdbc:tdsql-mysql:loadbalance://9.30.2.116:15012/test" +
            "?tdsqlLoadBalanceStrategy=sed" +
            "&useLocalSessionStates=true" +
            "&useUnicode=true" +
            "&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai" +
            "&logger=Slf4JLogger" +
            "&autoReconnect=true&socketTimeout=1000";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";
//    private static final DruidDataSource ds1 = new DruidDataSource();
    private static final DruidDataSource ds2 = new DruidDataSource();
    private static final DruidDataSource ds3 = new DruidDataSource();
    private static HikariDataSource ds1 = new HikariDataSource();

    static {
        try {
            Class.forName(DRIVER_NAME);
            /*ds1.setName("ds1");
            ds1.setUrl(DB_URL_1);
            ds1.setUsername(USERNAME);
            ds1.setPassword(PASSWORD);
            ds1.setDriverClassName(DRIVER_NAME);
            ds1.setInitialSize(1);
            ds1.setMaxActive(2);
            ds1.setMinIdle(1);
            ds1.setValidationQuery("select 1");
            ds1.setTimeBetweenEvictionRunsMillis(10000);
            ds1.setTestWhileIdle(true);
            ds1.setPhyTimeoutMillis(20000);
            ds1.setKeepAlive(true);
            ds1.init();*/
            HikariConfig config = new HikariConfig();
            config.setPoolName("ds1");
            config.setDriverClassName(DRIVER_NAME);
            config.setJdbcUrl(DB_URL_1);
            config.setUsername(USERNAME);
            config.setPassword(PASSWORD);
            config.setMinimumIdle(30);
            config.setMaximumPoolSize(30);
            config.setMaxLifetime(30000);
            ds1 = new HikariDataSource(config);
            /*ds2.setName("ds2");
            ds2.setUrl(DB_URL_2);
            ds2.setUsername(USERNAME);
            ds2.setPassword(PASSWORD);
            ds2.setDriverClassName(DRIVER_NAME);
            ds2.setInitialSize(1);
            ds2.setMaxActive(1);
            ds2.setMinIdle(1);
            ds2.setValidationQuery("select 1");
            ds2.setTimeBetweenEvictionRunsMillis(10000);
            ds2.setTestWhileIdle(true);
            ds2.setPhyTimeoutMillis(20000);
            ds2.init();
            ds3.setName("ds3");
            ds3.setUrl(DB_URL_3);
            ds3.setUsername(USERNAME);
            ds3.setPassword(PASSWORD);
            ds3.setDriverClassName(DRIVER_NAME);
            ds3.setInitialSize(1);
            ds3.setMaxActive(1);
            ds3.setMinIdle(1);
            ds3.setValidationQuery("select 1");
            ds3.setTimeBetweenEvictionRunsMillis(10000);
            ds3.setTestWhileIdle(true);
            ds3.setPhyTimeoutMillis(20000);
            ds3.init();*/
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        /*ThreadPoolExecutor executorService = new ThreadPoolExecutor(100, 100, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());*/
        ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(
                () -> System.out.println(
                        "Time: " + LocalTime.now()
                                + ", Active Size: " + executorService.getActiveCount()
                                + ", Pool size: " + executorService.getPoolSize()
                                + ", Task count: " + executorService.getTaskCount()
                                + ", queue Size: " + executorService.getQueue().size()
                                + ", finish count: " + executorService.getCompletedTaskCount()
                                /*+ ", ds1 Active: " + ds1.getActiveCount() + ", ds1 create: " + ds1.getCreateCount()
                                + ", ds1 connect: " + ds1.getConnectCount()*/
                                + ", ds1 Active: " + ds1.getHikariPoolMXBean().getActiveConnections() + ", ds1 create: " + ds1.getHikariPoolMXBean().getTotalConnections()
                                + ", ds1 idle: " + ds1.getHikariPoolMXBean().getIdleConnections()
                                + ", ds2 Active: " + ds2.getActiveCount() + ", ds2 create: " + ds2.getCreateCount()
                                + ", ds2 connect: " + ds2.getConnectCount()
                                + ", ds3 Active: " + ds3.getActiveCount() + ", ds3 create: " + ds3.getCreateCount()
                                + ", ds3 connect: " + ds3.getConnectCount()
                ), 0, 1000, TimeUnit.MILLISECONDS);

        while (true) {
            try {
                executorService.execute(new QueryTask(ds1));
                TimeUnit.MILLISECONDS.sleep(20);
                /*executorService.execute(new QueryTask(ds2));
                TimeUnit.MILLISECONDS.sleep(20);
                executorService.execute(new QueryTask(ds3));
                TimeUnit.MILLISECONDS.sleep(20);*/
            } catch (Exception e) {
                System.err.println(" 1.======================= " + e.getMessage());
            }
        }
    }

    private static class QueryTask implements Runnable {

        private final DataSource ds;

        public QueryTask(DataSource ds) {
            this.ds = ds;
        }

        @Override
        public void run() {
            try (Connection conn = ds.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select count(*) from t1;")
            ) {
                while (rs.next()) {
                    /*if (ds instanceof DruidDataSource) {
                        System.out.println(((DruidDataSource)ds).getName() + " = " + rs.getInt(1));
                    } else if (ds instanceof HikariDataSource) {
                        System.out.println(((HikariDataSource) ds).getPoolName() + " = " + rs.getInt(1));
                    }*/
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (Exception e) {
                System.err.println(" 2.======================= " + e.getMessage());
            }
        }
    }
}
