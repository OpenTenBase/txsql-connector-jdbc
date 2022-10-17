package tdsql.loadbalance.base;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_INITIALSIZE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MAXACTIVE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MINIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_PASSWORD;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONBORROW;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONRETURN;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTWHILEIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_USERNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_VALIDATIONQUERY;
import static com.alibaba.druid.pool.DruidDataSourceFactory.createDataSource;

import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlXADataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class BaseTest {

    protected static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    protected static final String PROXY_16 = "tdsqlshard-j9cybzl8.sql.tencentcdb.com:16";
    protected static final String PROXY_44 = "tdsqlshard-axvsyeas.sql.tencentcdb.com:44";
    protected static final String PROXY_46 = "tdsqlshard-e07e0ois.sql.tencentcdb.com:46";
    protected static final String PROXY_48 = "tdsqlshard-p9or8etq.sql.tencentcdb.com:48";

    protected static final String[] PROXY_ARRAY = {PROXY_16, PROXY_44, PROXY_46, PROXY_48};
    protected static final String DB_MYSQL = "mysql";
    protected static final String LB_URL_PROPS = "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&logger=Slf4JLogger"
            + "&tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";
    protected static final String LB_URL =
            "jdbc:tdsql-mysql:loadbalance://" + PROXY_16 + "," + PROXY_44 + "," + PROXY_46 + "," + PROXY_48 + "/"
                    + DB_MYSQL + LB_URL_PROPS;
    protected static final String USER = "tdsqluser";
    protected static final String PASS = "Tdsql@2022";

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.printf("Running test: %s, method: %s%n",
                testInfo.getTestClass().orElse(Class.forName("java.lang.NullPointerException")).getName(),
                testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        Class.forName(DRIVER_CLASS_NAME);
    }

    protected DataSource createMysqlDataSource() {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL(LB_URL);
        ds.setUser(USER);
        ds.setPassword(PASS);
        return ds;
    }

    protected DataSource createHikariDataSource(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_CLASS_NAME);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    protected DataSource createHikariDataSource(String jdbcUrl) {
        return this.createHikariDataSource(jdbcUrl, USER, PASS);
    }

    protected DataSource createHikariDataSource() {
        return this.createHikariDataSource(LB_URL);
    }

    protected DataSource createDruidDataSource(String jdbcUrl, String username, String password) throws Exception {
        Properties prop = new Properties();
        prop.setProperty(PROP_DRIVERCLASSNAME, DRIVER_CLASS_NAME);
        prop.setProperty(PROP_URL, jdbcUrl);
        prop.setProperty(PROP_USERNAME, username);
        prop.setProperty(PROP_PASSWORD, password);
        prop.setProperty(PROP_INITIALSIZE, "10");
        prop.setProperty(PROP_MINIDLE, "10");
        prop.setProperty(PROP_MAXACTIVE, "10");
        prop.setProperty(PROP_TESTONBORROW, "false");
        prop.setProperty(PROP_TESTONRETURN, "false");
        prop.setProperty(PROP_TESTWHILEIDLE, "true");
        prop.setProperty(PROP_VALIDATIONQUERY, "select 1");
        return createDataSource(prop);
    }

    protected DataSource createDruidDataSource(String jdbcUrl) throws Exception {
        return this.createDruidDataSource(jdbcUrl, USER ,PASS);
    }

    protected DataSource createDruidDataSource(Properties prop) throws Exception {
        return createDataSource(prop);
    }

    protected DataSource createDruidDataSource() throws Exception {
        return this.createDruidDataSource(LB_URL);
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName, String jdbcUrl, String username,
            String password) throws SQLException {
        MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
        mysqlXADataSource.setUrl(jdbcUrl);
        mysqlXADataSource.setUser(username);
        mysqlXADataSource.setPassword(password);

        AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
        ds.setXaProperties(new Properties());
        ds.setXaDataSource(mysqlXADataSource);
        ds.setUniqueResourceName(uniqueResourceName);
        ds.setXaDataSourceClassName("com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlXADataSource");
        ds.setMinPoolSize(10);
        ds.setMaxPoolSize(10);
        ds.setMaxLifetime(20000);
        ds.setBorrowConnectionTimeout(10000);
        ds.setLoginTimeout(3);
        ds.setMaintenanceInterval(100);
        ds.setMaxIdleTime(300);
        ds.setTestQuery("select 1");
        return ds;
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName, String jdbcUrl) throws SQLException {
        return this.createAtomikosDataSource(uniqueResourceName, jdbcUrl, USER, PASS);
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName) throws SQLException {
        return this.createAtomikosDataSource(uniqueResourceName, LB_URL);
    }

    protected DataSource createAtomikosDataSource() throws SQLException {
        return this.createAtomikosDataSource("ds1");
    }

    protected void warmUp(DataSource dataSource, int threadCnt, int taskCnt, int waitSec) {
        try {
            ExecutorService pool = Executors.newFixedThreadPool(threadCnt);
            CountDownLatch latch = new CountDownLatch(taskCnt);
            for (int i = 0; i < taskCnt; i++) {
                pool.execute(() -> {
                    try (Connection conn = dataSource.getConnection();
                            Statement stmt = conn.createStatement()) {
                        stmt.executeQuery("show processlist;");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            boolean await = latch.await(waitSec, TimeUnit.SECONDS);
            if (!await) {
                pool.shutdownNow();
            } else {
                pool.shutdown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
