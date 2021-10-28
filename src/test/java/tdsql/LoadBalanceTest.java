package tdsql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author dorianzhang@tencent.com
 */
public class LoadBalanceTest {

    private static final String DB_CLASS = "com.tencent.tdsql.mysql.cj.jdbc.Driver";

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DB_CLASS);
    }

    @Test
    public void case01() throws InterruptedException {
        DataSource ds = this.createHikariDataSource();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                100, 100, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000000), new ThreadPoolExecutor.AbortPolicy());

        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledExecutor.scheduleAtFixedRate(() -> System.out.println(
                "Time: " + LocalTime.now() +
                        ", Pool Size: " + executor.getPoolSize()
                        + ", Queue Size: " + executor.getQueue().size()
        ), 0, 1000, TimeUnit.MILLISECONDS);

        while (true) {
            executor.execute(new SelectOneTask(ds));
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }

    private DataSource createHikariDataSource() {
        int min = 20;
        HikariDataSource ds = new HikariDataSource();
        ds.setUsername("root");
        ds.setPassword("123456");
        ds.setMinimumIdle(min);
        ds.setMaximumPoolSize(min);
        ds.setMaxLifetime(30000);
        ds.setJdbcUrl(
                "jdbc:tdsql-mysql:loadbalance://9.134.209.89:3357,9.134.209.89:3358,9.134.209.89:3359,9.134.209.89:3360/jdbc_test_db"
                        + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=1200"
                        + "&haLoadBalanceStrategy=sed"
                        + "&haLoadBalanceWeightFactor=1,1,1,1"
                        + "&haLoadBalanceBlacklistTimeout=5000"
                        + "&haLoadBalanceHeartbeatMonitor=true"
                        + "&haLoadBalanceHeartbeatIntervalTime=3000"
                        + "&haLoadBalanceMaximumErrorRetries=1");
        return ds;
    }

    private static class SelectOneTask implements Runnable {

        private final DataSource ds;

        public SelectOneTask(DataSource ds) {
            this.ds = ds;
        }

        @Override
        public void run() {
            Connection conn = null;
            PreparedStatement psmt = null;
            try {
                conn = ds.getConnection();
                conn.setAutoCommit(false);
                psmt = conn.prepareStatement("select id,name from t_user where id=258927;");
                psmt.executeQuery();
                psmt = conn.prepareStatement("select id,name from t_user where id=259003;");
                psmt.executeQuery();
                psmt = conn.prepareStatement("update t_user set name = ? where id=258927;");
                psmt.setString(1, UUID.randomUUID().toString());
                psmt.executeUpdate();
                psmt = conn.prepareStatement("update java8_test set name = ? where id=259003;");
                psmt.setString(1, UUID.randomUUID().toString());
                psmt.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                // do nothing
            } finally {
                if (psmt != null) {
                    try {
                        psmt.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
