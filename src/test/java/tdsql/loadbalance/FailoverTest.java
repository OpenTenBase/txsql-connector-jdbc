package tdsql.loadbalance;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_INITIALSIZE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MAXACTIVE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MINIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_PASSWORD;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_PHY_TIMEOUT_MILLIS;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONBORROW;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONRETURN;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTWHILEIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_USERNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_VALIDATIONQUERY;

import com.alibaba.druid.pool.DruidDataSource;
import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class FailoverTest extends BaseTest {

    private String jdbcUrl = "jdbc:tdsql-mysql:loadbalance:" +
            "//9.30.0.250:15023,9.30.2.116:15023/test" +
            "?tdsqlLoadBalanceStrategy=sed" +
            "&logger=Slf4JLogger" +
            "&tdsqlLoadBalanceWeightFactor=1,1" +
            "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
            "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000";

    @Test
    public void case01() throws Exception {
        Properties prop = new Properties();
        prop.setProperty(PROP_DRIVERCLASSNAME, DRIVER_CLASS_NAME);
        prop.setProperty(PROP_URL, jdbcUrl);
        prop.setProperty(PROP_USERNAME, "qt4s");
        prop.setProperty(PROP_PASSWORD, "g<m:7KNDF.L1<^1C");
        prop.setProperty(PROP_INITIALSIZE, "20");
        prop.setProperty(PROP_MINIDLE, "20");
        prop.setProperty(PROP_MAXACTIVE, "20");
        prop.setProperty(PROP_TESTONBORROW, "false");
        prop.setProperty(PROP_TESTONRETURN, "false");
        prop.setProperty(PROP_TESTWHILEIDLE, "true");
        prop.setProperty(PROP_VALIDATIONQUERY, "select 1");
        prop.setProperty(PROP_PHY_TIMEOUT_MILLIS, "30000");
        DruidDataSource ds = (DruidDataSource) createDruidDataSource(prop);

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_CLASS_NAME);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername("qt4s");
        config.setPassword("g<m:7KNDF.L1<^1C");
        config.setMinimumIdle(20);
        config.setMaximumPoolSize(20);
        config.setMaxLifetime(30000);
        //        HikariDataSource ds = new HikariDataSource(config);

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                //                System.out.println("======================== Active: " + ds.getHikariPoolMXBean().getActiveConnections());
                //                System.out.println("======================== Total:  " + ds.getHikariPoolMXBean().getTotalConnections());
                //                System.out.println("======================== Idle:   " + ds.getHikariPoolMXBean().getIdleConnections());
                System.out.println("ds.getCreateCount() = " + ds.getCreateCount());
                System.out.println("ds.getActiveCount() = " + ds.getActiveCount());
                System.out.println("ds.getDiscardCount() = " + ds.getDiscardCount());
            }
        }, 0, 1, TimeUnit.SECONDS);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(100, 100, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        while (true) {
            executor.submit(new QueryTask(ds));
            TimeUnit.MILLISECONDS.sleep(2);
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
                    Statement stmt = conn.createStatement()) {
                conn.setAutoCommit(false);
                ResultSet rs = stmt.executeQuery("select count(*) from t_user;");
                while (rs.next()) {
                    TimeUnit.MILLISECONDS.sleep(1);
                }
                conn.commit();
                rs.close();
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
