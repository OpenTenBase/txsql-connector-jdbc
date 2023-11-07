package tdsql.loadbalance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.druid.pool.DruidDataSource;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.loadbalance.base.BaseTest;
import testsuite.util.InstanceInfo;
import testsuite.util.InstanceOp;
import testsuite.util.Undo;

import javax.sql.DataSource;

/**
 * 此单元测试类内的每个单元测试方法需要单独独立执行
 */
@TestMethodOrder(OrderAnnotation.class)
public class MultiDataSourceTest extends BaseTest {

    private InstanceInfo instanceInfo = produceInstanceInfo();

    @Test
    @Order(1)
    public void testMultiHikari() throws Exception {
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

    private void testMultiHikari(String[] jdbcUrl) throws Exception {
        HikariDataSource ds1 = null;
        HikariDataSource ds2 = null;

        try {
            recoverAllProxy(instanceInfo);
            ds1 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[0], USER, PASS);
            ds2 = (HikariDataSource) super.createHikariDataSource(jdbcUrl[1], USER, PASS);
            testMultiDatasources(ds1, ds2);
        }  finally {
            if (ds1 != null) {
                ds1.close();
            }
            if (ds2 != null) {
                ds2.close();
            }
        }
    }

    private void testMultiDatasources(DataSource ds1, DataSource ds2) throws Exception {

        Undo undo = null;
        List<Undo> undoList = null;
        try {
            int threadNum = 5;
            ThreadPoolExecutor executor = initThreadPool(threadNum * 2, threadNum * 2);
            List<QueryTask> queryTasks = createQueryTasks(ds1, "ds1", executor, threadNum);
            queryTasks.addAll(createQueryTasks(ds2, "ds2", executor, threadNum));

            TimeUnit.SECONDS.sleep(10);
            boolean status = validateQueryTask(queryTasks);
            if (!status) {
                throw new RuntimeException("Not all query tasks is in successful status!");
            }
            undo =faioverOneProxy(instanceInfo);
            TimeUnit.SECONDS.sleep(20);
            status = validateQueryTask(queryTasks);
            if (!status) {
                throw new RuntimeException("Not all query tasks is in successful status! Even if one proxy has failed down!");
            }

            undo.undo();
            undo = null;
            TimeUnit.SECONDS.sleep(10);
            status = validateQueryTask(queryTasks);
            if (!status) {
                throw new RuntimeException("Not all query tasks is in successful status! The faildown proxy has recovered!");
            }

            undoList = failoverAllProxy(instanceInfo);
            TimeUnit.SECONDS.sleep(10);
            System.out.println("recover all ip port: ");
            undoList.forEach(Undo::undo);
            TimeUnit.SECONDS.sleep(20);
            status = validateQueryTask(queryTasks);
            if (!status) {
                throw new RuntimeException("Not all query tasks is in successful status! All proxies were failed, and then recovered!");
            }
        } finally {
            if (undo != null) {
                undo.undo();
            }
            if (undoList != null) {
                undoList.forEach(Undo::undo);
            }
        }
    }

    private void testMultiDruid(String[] jdbcUrl) throws Exception {
        DruidDataSource ds1 = null;
        DruidDataSource ds2 = null;
        try {
            recoverAllProxy(instanceInfo);
            ds1 = initDruidDataSource(jdbcUrl[0]);
            ds2 = initDruidDataSource(jdbcUrl[1]);
            testMultiDatasources(ds1, ds2);
        } finally {
            if (ds1 != null)
                ds1.close();
            if (ds2 != null)
                ds2.close();
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

        TimeUnit.MINUTES.sleep(2);

        ds1.close();
        ds2.close();
    }
}
