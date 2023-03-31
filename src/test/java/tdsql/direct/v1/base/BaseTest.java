package tdsql.direct.v1.base;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_INITIALSIZE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MAXACTIVE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MINIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_PASSWORD;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_USERNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.createDataSource;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RW;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
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
public abstract class BaseTest {

    protected static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    /**
     * protected static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
     *     protected static final String URL_RW = "jdbc:tdsql-mysql:direct://"
     *             + "9.30.1.231:15006,"
     *             + "/mysql?useSSL=false&tdsqlReadWriteMode=rw&tdsqlLoadBalanceStrategy=Lc";
     *     protected static final String URL_RO = "jdbc:tdsql-mysql:direct://"
     *             + "9.30.1.231:15006,"
     *             + "/mysql?useSSL=false&tdsqlReadWriteMode=ro&tdsqlLoadBalanceStrategy=Lc&tdsqlDirectMasterCarryOptOfReadOnlyMode=true";
     *     protected static final String USER = "tdsqlsys_normal";
     *     protected static final String PASS = "5R77aqf9kSk8HnN%R";
     *
     */
    protected static final String URL_RW = "jdbc:mysql:direct://"
        + "9.30.0.250:15012,"
        + "/test?useSSL=false&tdsqlReadWriteMode=rw";
    protected static final String URL_RO = "jdbc:mysql:direct://"
            + "9.30.0.250:15012,"
            + "/test?useSSL=false&tdsqlReadWriteMode=ro&tdsqlMaxSlaveDelay=12.9";
    protected static final String USER_RW = "qt4s";
    protected static final String PASS_RW = "g<m:7KNDF.L1<^1C";
    protected static final String USER_RO = "qt4s_ro";
    protected static final String PASS_RO = "g<m:7KNDF.L1<^1C";

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.printf("Running test: %s, method: %s%n",
                testInfo.getTestClass().orElse(Class.forName("java.lang.NullPointerException")).getName(),
                testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        Class.forName(DRIVER_CLASS_NAME);
    }

    protected Connection getConnection(TdsqlDirectReadWriteModeEnum mode, String user) throws SQLException {
        Properties props = new Properties();
        if (USER_RO.equalsIgnoreCase(user)) {
            props.setProperty(PropertyKey.USER.getKeyName(), USER_RO);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS_RO);
        } else {
            props.setProperty(PropertyKey.USER.getKeyName(), USER_RW);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS_RW);
        }
        return getConnection(mode, props);
    }

    protected Connection getConnection(TdsqlDirectReadWriteModeEnum mode, Properties properties) throws SQLException {
        Properties props = new Properties();
        if (RO.equals(mode)) {
            props.setProperty(PropertyKey.USER.getKeyName(), USER_RO);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS_RO);
            props.setProperty(PropertyKey.tdsqlDirectReadWriteMode.getKeyName(), RO.getRwModeName());
        } else {
            props.setProperty(PropertyKey.USER.getKeyName(), USER_RW);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS_RW);
            props.setProperty(PropertyKey.tdsqlDirectReadWriteMode.getKeyName(), RW.getRwModeName());
        }
        props.putAll(properties);
        return DriverManager.getConnection(URL_RW, props);
    }

    protected void printAllConnection() {
        int total = 0;
//        for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : TdsqlDirectConnectionManager.getInstance()
//                .getAllConnection().entrySet()) {
//            for (JdbcConnection jdbcConnection : entry.getValue()) {
//                ++total;
//                System.out.println("Host:Connection = " + entry.getKey() + ": " + jdbcConnection);
//            }
//        }
//        System.out.println("Total: " + total);
    }

    protected void printScheduleQueue() {
        long total = 0;
//        for (Entry<TdsqlHostInfo, NodeMsg> entry : TdsqlDirectTopoServer.getInstance().getScheduleQueue().asMap()
//                .entrySet()) {
//            total += entry.getValue().getCount();
//            System.out.println("Host:Count = " + entry.getKey() + ": " + entry.getValue());
//        }
//        System.out.println("Total: " + total);
    }

    protected DataSource createMysqlDataSource() {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL(URL_RW);
        ds.setUser(USER_RW);
        ds.setPassword(PASS_RW);
        return ds;
    }

    protected DataSource createHikariDataSource() {
        return createHikariDataSource(10, 10, RW);
    }

    protected DataSource createHikariDataSource(int min, int max, TdsqlDirectReadWriteModeEnum mode) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_CLASS_NAME);
        if (RO.equals(mode)) {
            config.setJdbcUrl(URL_RO);
            config.setUsername(USER_RO);
            config.setPassword(PASS_RO);
        } else {
            config.setJdbcUrl(URL_RW);
            config.setUsername(USER_RW);
            config.setPassword(PASS_RW);
        }
        config.setMinimumIdle(min);
        config.setMaximumPoolSize(max);
        return new HikariDataSource(config);
    }

    protected DataSource createDruidDataSource() throws Exception {
        return createDruidDataSource(10, 10, 10, RW);
    }

    protected DataSource createDruidDataSource(int init, int min, int max, TdsqlDirectReadWriteModeEnum mode)
            throws Exception {
        Properties prop = new Properties();
        prop.setProperty(PROP_DRIVERCLASSNAME, DRIVER_CLASS_NAME);
        if (RO.equals(mode)) {
            prop.setProperty(PROP_URL, URL_RO);
            prop.setProperty(PROP_USERNAME, USER_RO);
            prop.setProperty(PROP_PASSWORD, PASS_RO);
        } else {
            prop.setProperty(PROP_URL, URL_RW);
            prop.setProperty(PROP_USERNAME, USER_RW);
            prop.setProperty(PROP_PASSWORD, PASS_RW);
        }
        prop.setProperty(PROP_INITIALSIZE, String.valueOf(init));
        prop.setProperty(PROP_MINIDLE, String.valueOf(min));
        prop.setProperty(PROP_MAXACTIVE, String.valueOf(max));
        return createDataSource(prop);
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
