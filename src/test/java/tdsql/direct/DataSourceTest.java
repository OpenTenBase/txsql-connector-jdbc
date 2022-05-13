package tdsql.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.alibaba.druid.pool.DruidDataSource;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import tdsql.base.BaseTest;

public class DataSourceTest extends BaseTest {

    @Test
    public void testCreateMysqlDataSource() throws SQLException {
        DataSource dataSource = createMysqlDataSource();
        assertNotNull(dataSource);

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show processlist;")) {
            assertNotNull(conn);
            assertNotNull(stmt);
            assertNotNull(rs);
        }
    }

    @Test
    public void testCreateHikariDataSource() {
        HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource();
        assertNotNull(hikariDataSource);

        int max = hikariDataSource.getMaximumPoolSize();
        warmUp(hikariDataSource, max * 2, max * 100);

        HikariPoolMXBean mxBean = hikariDataSource.getHikariPoolMXBean();
        assertEquals(10, mxBean.getTotalConnections());
        assertEquals(0, mxBean.getActiveConnections());
        assertEquals(10, mxBean.getIdleConnections());

        hikariDataSource.close();
    }

    @Test
    public void testCreateDruidDataSource() throws Exception {
        DruidDataSource druidDataSource = (DruidDataSource) createDruidDataSource();
        assertNotNull(druidDataSource);

        int max = druidDataSource.getMaxActive();
        warmUp(druidDataSource, max * 2, max * 100);

        assertEquals(10, druidDataSource.getCreateCount());
        assertEquals(0, druidDataSource.getActiveCount());
        assertEquals(10, max);

        druidDataSource.close();
    }
}
