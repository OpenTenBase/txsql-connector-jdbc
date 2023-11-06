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
import java.util.concurrent.TimeUnit;
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
    public void testMultiHikari() {
        this.testMultiHikari(new String[]{
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_2 + "/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_3 + "/qt4s" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=2,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true"
        });
    }

    @Test
    @Order(2)
    public void testMultiDruid() throws Exception {
        this.testMultiDruid(new String[]{
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_2 + "/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_3 + "/qt4s" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=2,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true"
        });
    }

    @Test
    @Order(5)
    public void testMultiAtomikos() throws SQLException, InterruptedException {
        this.testMultiAtomikos(new String[]{
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_2 + "/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//" + PROXY_1 + "," + PROXY_3 + "/qt4s" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=2,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true"
        });
    }

    private void testMultiHikari(String[] jdbcUrl) {
        HikariDataSource ds1 = null;
        HikariDataSource ds2 = null;
        if (jdbcUrl.length == 0) {
            ds1 = (HikariDataSource) super.createHikariDataSource();
            ds2 = (HikariDataSource) super.createHikariDataSource();
        } else if (jdbcUrl.length == 2) {
            ds1 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[0], USER, PASS);
            ds2 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[1], USER, PASS);
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

            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
            ds1 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[0], USER, PASS);
            ds2 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[1], USER, PASS);
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

            TimeUnit.MINUTES.sleep(5);
        } finally {
            if (ds1 != null) {
                ds1.close();
            }
            if (ds2 != null) {
                ds2.close();
            }
        }
    }

    private void testMultiAtomikos(String[] jdbcUrl) throws SQLException, InterruptedException {
        AtomikosDataSourceBean ds1 = null;
        AtomikosDataSourceBean ds2 = null;
        if (jdbcUrl.length == 0) {
            ds1 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds1");
            ds2 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds2");
        } else if (jdbcUrl.length == 2) {
            ds1 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds1", jdbcUrl[0], USER, PASS);
            ds2 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds2", jdbcUrl[1], USER, PASS);
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

        TimeUnit.MINUTES.sleep(5);

        ds1.close();
        ds2.close();
    }
}
