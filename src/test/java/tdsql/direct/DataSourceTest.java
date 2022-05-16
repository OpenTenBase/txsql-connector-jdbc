package tdsql.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.alibaba.druid.pool.DruidDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.base.BaseTest;

@TestMethodOrder(OrderAnnotation.class)
public class DataSourceTest extends BaseTest {

    @Test
    @Order(1)
    public void testCreateMysqlDataSource() throws SQLException {
        MysqlDataSource mysqlDataSource = (MysqlDataSource) createMysqlDataSource();
        assertNotNull(mysqlDataSource);

        try (Connection conn = mysqlDataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("show processlist;")) {
            assertNotNull(conn);
            assertNotNull(stmt);
            assertNotNull(rs);
        }
    }

    @Test
    @Order(2)
    public void testCreateHikariDataSource() {
        try (HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource()) {
            assertNotNull(hikariDataSource);

            int max = hikariDataSource.getMaximumPoolSize();
            warmUp(hikariDataSource, max * 2, max * 100, max * 3);

            HikariPoolMXBean mxBean = hikariDataSource.getHikariPoolMXBean();
            assertEquals(max, mxBean.getTotalConnections());
            assertEquals(0, mxBean.getActiveConnections());
            assertEquals(max, mxBean.getIdleConnections());
        }
    }

    @Test
    @Order(3)
    public void testCreateDruidDataSource() throws Exception {
        try (DruidDataSource druidDataSource = (DruidDataSource) createDruidDataSource()) {
            assertNotNull(druidDataSource);

            int max = druidDataSource.getMaxActive();
            warmUp(druidDataSource, max * 2, max * 100, max * 3);

            assertEquals(max, druidDataSource.getCreateCount());
            assertEquals(0, druidDataSource.getActiveCount());
            assertEquals(max, druidDataSource.getPoolingCount());
        }
    }
}
