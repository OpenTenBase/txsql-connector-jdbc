package tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.alibaba.druid.pool.DruidDataSource;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.base.BaseTest;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
@TestMethodOrder(OrderAnnotation.class)
public class LoadBalanceTest extends BaseTest {

    @Test
    @Order(1)
    public void testHikariPool() {
        int min = 100;
        int max = 100;

        try (HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource(min, max, RO)) {
            assertNotNull(hikariDataSource);

            warmUp(hikariDataSource, max * 2, max * 1000, max * 3);

            HikariPoolMXBean mxBean = hikariDataSource.getHikariPoolMXBean();
            assertEquals(max, mxBean.getTotalConnections());
            assertEquals(0, mxBean.getActiveConnections());
            assertEquals(max, mxBean.getIdleConnections());
        }
    }

    @Test
    @Order(2)
    public void testDruidPool() {
        int init = 100;
        int min = 100;
        int max = 100;

        Assertions.assertDoesNotThrow(() -> {
            try (DruidDataSource druidDataSource = (DruidDataSource) createDruidDataSource(init, min, max, RO)) {
                assertNotNull(druidDataSource);

                warmUp(druidDataSource, max * 2, max * 1000, max * 3);
                assertEquals(max, druidDataSource.getCreateCount());
                assertEquals(0, druidDataSource.getActiveCount());
                assertEquals(max, druidDataSource.getPoolingCount());
            }
        });
    }
}
