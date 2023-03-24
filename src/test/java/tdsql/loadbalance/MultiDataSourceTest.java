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
                        "//9.30.0.250:15012,9.30.2.116:15012/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//9.30.2.89:15012,9.30.2.94:15012/qt4s" +
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
                        "//9.30.0.250:15012,9.30.2.116:15012/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//9.30.0.250:15012,9.30.2.116:15012/qt4s" +
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
                        "//9.30.0.250:15023,9.30.2.116:15012/test" +
                        "?tdsqlLoadBalanceStrategy=sed" +
                        "&logger=Slf4JLogger" +
                        "&tdsqlLoadBalanceWeightFactor=1,1" +
                        "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                        "&tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis=100" +
                        "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=3000" +
                        "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                        "&autoReconnect=true",
                "jdbc:tdsql-mysql:loadbalance:" +
                        "//9.30.0.250:15023,9.30.2.116:15012/qt4s" +
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
            ds1 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[0], "qt4s", "g<m:7KNDF.L1<^1C");
            ds2 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[1], "qt4s", "g<m:7KNDF.L1<^1C");
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

            TimeUnit.MINUTES.sleep(10);
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
            ds1 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[0], "qt4s", "g<m:7KNDF.L1<^1C");
            ds2 = (DruidDataSource) super.createDruidDataSource(jdbcUrl[1], "qt4s", "g<m:7KNDF.L1<^1C");
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

            TimeUnit.MINUTES.sleep(10);
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
            ds1 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds1", jdbcUrl[0], "qt4s", "g<m:7KNDF.L1<^1C");
            ds2 = (AtomikosDataSourceBean) super.createAtomikosDataSource("ds2", jdbcUrl[1], "qt4s", "g<m:7KNDF.L1<^1C");
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

        TimeUnit.MINUTES.sleep(10);

        ds1.close();
        ds2.close();
    }
}
