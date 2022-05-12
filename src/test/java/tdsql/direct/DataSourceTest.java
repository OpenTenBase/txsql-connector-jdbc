package tdsql.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataSourceTest {

    @Test
    public void testCreateDataSource() throws SQLException {
        DataSource ds = createMysqlDataSource();
        Assertions.assertNotNull(ds);

        try (Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show processlist;")) {
            Assertions.assertNotNull(conn);
            Assertions.assertNotNull(stmt);

            while (rs.next()) {
                System.out.println(rs.getString(1)
                        + " " + rs.getString(2)
                        + " " + rs.getString(3)
                        + " " + rs.getString(4)
                        + " " + rs.getString(5)
                        + " " + rs.getString(6)
                        + " " + rs.getString(7)
                        + " " + rs.getString(8));
            }
        }
    }

    @Test
    public void testCreateHikariDataSource() throws SQLException {
        HikariDataSource hikariDs = (HikariDataSource) createHikariDataSource(DEFAULT_URL);
        Assertions.assertNotNull(hikariDs);

        try (Connection conn = hikariDs.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show processlist;")) {
            Assertions.assertNotNull(conn);
            Assertions.assertNotNull(stmt);

            while (rs.next()) {
                System.out.println(rs.getString(1)
                        + " " + rs.getString(2)
                        + " " + rs.getString(3)
                        + " " + rs.getString(4)
                        + " " + rs.getString(5)
                        + " " + rs.getString(6)
                        + " " + rs.getString(7)
                        + " " + rs.getString(8));
            }
        } finally {
            hikariDs.close();
        }
    }

    @Test
    public void testCreateDruidDataSource() throws Exception {
        DruidDataSource druidDs = (DruidDataSource) createDruidDataSource(DEFAULT_URL);
        Assertions.assertNotNull(druidDs);

        try (Connection conn = druidDs.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show processlist;")) {
            Assertions.assertNotNull(conn);
            Assertions.assertNotNull(stmt);

            while (rs.next()) {
                System.out.println(rs.getString(1)
                        + " " + rs.getString(2)
                        + " " + rs.getString(3)
                        + " " + rs.getString(4)
                        + " " + rs.getString(5)
                        + " " + rs.getString(6)
                        + " " + rs.getString(7)
                        + " " + rs.getString(8));
            }
        } finally {
            druidDs.close();
        }
    }

    @Test
    public void testHikariDataSource() throws InterruptedException {
        HikariDataSource hikariDs = (HikariDataSource) createHikariDataSource(DEFAULT_URL + "?tdsqlReadWriteMode=ro");
        TimeUnit.SECONDS.sleep(5);
        assertEquals(10, hikariDs.getHikariPoolMXBean().getTotalConnections());

        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
        assertEquals(2, scheduleQueue.size());

        for (Entry<TdsqlHostInfo, Long> entry : scheduleQueue.asMap().entrySet()) {
            assertEquals(5, entry.getValue());
        }

        for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : TdsqlDirectConnectionManager.getInstance()
                .getAllConnection().entrySet()) {
            assertEquals(5, entry.getValue().size());
        }
    }

    @Test
    public void testDruidDataSource() throws Exception {
        DruidDataSource druidDs = (DruidDataSource) createDruidDataSource(DEFAULT_URL + "?tdsqlReadWriteMode=ro");
        TimeUnit.SECONDS.sleep(5);
        assertEquals(10, druidDs.getConnectCount());

        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
        assertEquals(2, scheduleQueue.size());

        for (Entry<TdsqlHostInfo, Long> entry : scheduleQueue.asMap().entrySet()) {
            assertEquals(10, entry.getValue());
        }

        for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : TdsqlDirectConnectionManager.getInstance()
                .getAllConnection().entrySet()) {
            assertEquals(10, entry.getValue().size());
        }
    }

    private DataSource createMysqlDataSource() {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL("jdbc:tdsql-mysql:direct://9.30.1.140:15038/test");
        ds.setUser("test");
        ds.setPassword("test");
        return ds;
    }

    private DataSource createHikariDataSource(String url) {
        int min = 10;
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        config.setJdbcUrl(url);
        config.setUsername("test");
        config.setPassword("test");
        config.setMinimumIdle(min);
        config.setMaximumPoolSize(min);
        return new HikariDataSource(config);
    }

    private DataSource createDruidDataSource(String url) throws Exception {
        Properties prop = new Properties();
        prop.setProperty(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        prop.setProperty(DruidDataSourceFactory.PROP_URL, url);
        prop.setProperty(DruidDataSourceFactory.PROP_USERNAME, "test");
        prop.setProperty(DruidDataSourceFactory.PROP_PASSWORD, "test");
        prop.setProperty(DruidDataSourceFactory.PROP_INITIALSIZE, "10");
        prop.setProperty(DruidDataSourceFactory.PROP_MAXACTIVE, "10");
        prop.setProperty(DruidDataSourceFactory.PROP_MINIDLE, "10");
        prop.setProperty(DruidDataSourceFactory.PROP_MAXWAIT, "6000");
        return DruidDataSourceFactory.createDataSource(prop);
    }

    private static final String DEFAULT_URL = "jdbc:tdsql-mysql:direct://9.30.1.140:15038/test";
}
