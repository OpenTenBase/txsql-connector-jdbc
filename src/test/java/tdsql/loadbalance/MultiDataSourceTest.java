package tdsql.loadbalance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.druid.pool.DruidDataSource;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.loadbalance.base.BaseTest;

/**
 * 此单元测试类内的每个单元测试方法需要单独独立执行
 */
@TestMethodOrder(OrderAnnotation.class)
public class MultiDataSourceTest extends BaseTest {

    @Test
    @Order(1)
    public void testMultiHikariWithSameUrl() {
        this.testMultiHikari(new String[]{});
    }

    @Test
    @Order(2)
    public void testMultiHikariWithDiffUrl() {
        String one = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_16, PROXY_44, DB_MYSQL,
                LB_URL_PROPS);
        String two = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_46, PROXY_48, DB_MYSQL,
                LB_URL_PROPS);
        this.testMultiHikari(new String[]{one, two});
    }

    @Test
    @Order(3)
    public void testMultiDruidWithSameUrl() throws Exception {
        this.testMultiDruid(new String[]{});
    }

    @Test
    @Order(4)
    public void testMultiDruidWithDiffUrl() throws Exception {
        String one = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_16, PROXY_44, DB_MYSQL,
                LB_URL_PROPS);
        String two = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_46, PROXY_48, DB_MYSQL,
                LB_URL_PROPS);
        this.testMultiDruid(new String[]{one, two});
    }

    @Test
    @Order(5)
    public void testMultiAtomikosWithSameUrl() throws SQLException {
        this.testMultiAtomikos(new String[]{});
    }

    @Test
    @Order(6)
    public void testMultiAtomikosWithDiffUrl() throws SQLException {
        String one = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_16, PROXY_44, DB_MYSQL,
                LB_URL_PROPS);
        String two = String.format("jdbc:tdsql-mysql:loadbalance://%s,%s/%s%s", PROXY_46, PROXY_48, DB_MYSQL,
                LB_URL_PROPS);
        this.testMultiAtomikos(new String[]{one, two});
    }

    private void testMultiHikari(String[] jdbcUrl) {
        HikariDataSource ds1 = null;
        HikariDataSource ds2 = null;
        if (jdbcUrl.length == 0) {
            ds1 = (HikariDataSource) super.createHikariDataSource();
            ds2 = (HikariDataSource) super.createHikariDataSource();
        } else if (jdbcUrl.length == 2) {
            ds1 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[0]);
            ds2 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[1]);
        }

        try {
            assertNotNull(ds1);
            assertNotNull(ds2);

            int max = ds1.getMaximumPoolSize();
            warmUp(ds1, max * 2, max * 100, max * 3);
            HikariPoolMXBean mxBean = ds1.getHikariPoolMXBean();
            assertEquals(max, mxBean.getTotalConnections());
            assertEquals(0, mxBean.getActiveConnections());
            assertEquals(max, mxBean.getIdleConnections());

            max = ds2.getMaximumPoolSize();
            warmUp(ds2, max * 2, max * 100, max * 3);
            mxBean = ds2.getHikariPoolMXBean();
            assertEquals(max, mxBean.getTotalConnections());
            assertEquals(0, mxBean.getActiveConnections());
            assertEquals(max, mxBean.getIdleConnections());
        } finally {
            if (ds1 != null) {
                ds1.close();
            }
            if (ds2 != null) {
                ds2.close();
            }
        }
    }

    private void testMultiDruid(String[] jdbcUrl) throws Exception {
        DruidDataSource ds1 = null;
        DruidDataSource ds2 = null;
        if (jdbcUrl.length == 0) {
            ds1 = (DruidDataSource) super.createDruidDataSource();
            ds2 = (DruidDataSource) super.createDruidDataSource();
        } else if (jdbcUrl.length == 2) {
            ds1 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[0]);
            ds2 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[1]);
        }

        try {
            assertNotNull(ds1);
            assertNotNull(ds2);

            int max = ds1.getMaxActive();
            warmUp(ds1, max * 2, max * 100, max * 3);
            assertEquals(max, ds1.getCreateCount());
            assertEquals(0, ds1.getActiveCount());
            assertEquals(max, ds1.getPoolingCount());

            max = ds2.getMaxActive();
            warmUp(ds2, max * 2, max * 100, max * 3);
            assertEquals(max, ds2.getCreateCount());
            assertEquals(0, ds2.getActiveCount());
            assertEquals(max, ds2.getPoolingCount());
        } finally {
            if (ds1 != null) {
                ds1.close();
            }
            if (ds2 != null) {
                ds2.close();
            }
        }
    }

    private void testMultiAtomikos(String[] jdbcUrl) throws SQLException {
        AtomikosDataSourceBean ds1 = null;
        AtomikosDataSourceBean ds2 = null;
        if (jdbcUrl.length == 0) {
            ds1 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds1");
            ds2 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds2");
        } else if (jdbcUrl.length == 2) {
            ds1 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds1", jdbcUrl[0]);
            ds2 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds2", jdbcUrl[1]);
        }

        assertNotNull(ds1);
        int max = ds1.getMaxPoolSize();
        warmUp(ds1, max * 2, max * 100, max * 3);
        assertTrue(ds1.getConcurrentConnectionValidation());
        Connection conn = ds1.getConnection();
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        assertTrue(conn.isValid(1));
        conn.close();

        assertNotNull(ds2);
        max = ds2.getMaxPoolSize();
        warmUp(ds2, max * 2, max * 100, max * 3);
        assertTrue(ds2.getConcurrentConnectionValidation());
        conn = ds2.getConnection();
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        assertTrue(conn.isValid(1));
        conn.close();

        ds1.close();
        ds2.close();
    }
}
