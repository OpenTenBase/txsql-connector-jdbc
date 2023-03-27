package tdsql.direct.v2;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectConnectionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 连接工厂单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConnectionFactoryTest extends TdsqlDirectBaseTest {

    /**
     * 创建1个连接 - 1个数据源 - 读写
     */
    @Test
    public void testCase01() {
        Assertions.assertDoesNotThrow(() -> {
            try (JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(
                    super.defaultConnectionUrlRw)) {
                Assertions.assertNotNull(jdbcConnection);
                Assertions.assertTrue(jdbcConnection.isValid(3));

                TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(
                        super.defaultConnectionUrlRw);
                Assertions.assertNotNull(dataSource);

                TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
                Assertions.assertNotNull(connectionManager);

                Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
                Assertions.assertNotNull(liveConnectionMap);
                Assertions.assertEquals(1, liveConnectionMap.size());
                Assertions.assertEquals(1, liveConnectionMap.keySet().size());

                List<JdbcConnection> allList = new ArrayList<>();
                for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                    allList.addAll(connectionList);
                }
                Assertions.assertEquals(1, allList.size());

                TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
                Assertions.assertNotNull(cacheServer);
                Assertions.assertTrue(cacheServer.getInitialCached());
                Assertions.assertFalse(cacheServer.getSurvived());
                Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

                TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
                Assertions.assertNotNull(masterCounter);
                Assertions.assertNotNull(masterCounter.getTdsqlHostInfo());
                Assertions.assertNotNull(masterCounter.getCount());
                Assertions.assertEquals(1L, masterCounter.getCount().longValue());
            }
        });

        TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(super.defaultConnectionUrlRw);
        Assertions.assertNotNull(dataSource);

        TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
        Assertions.assertNotNull(connectionManager);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
        Assertions.assertNotNull(liveConnectionMap);
        Assertions.assertEquals(0, liveConnectionMap.size());
        Assertions.assertEquals(0, liveConnectionMap.keySet().size());

        List<JdbcConnection> allList = new ArrayList<>();
        for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
            allList.addAll(connectionList);
        }
        Assertions.assertEquals(0, allList.size());

        TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
        Assertions.assertNotNull(cacheServer);
        Assertions.assertTrue(cacheServer.getInitialCached());
        Assertions.assertFalse(cacheServer.getSurvived());
        Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
        Assertions.assertNotNull(masterCounter);
        Assertions.assertNotNull(masterCounter.getTdsqlHostInfo());
        Assertions.assertNotNull(masterCounter.getCount());
        Assertions.assertEquals(0L, masterCounter.getCount().longValue());
    }

    /**
     * 创建500个连接 - 1个数据源 - 读写
     */
    @Test
    public void testCase02() {
        int connCount = 500;
        ExecutorService executorService = Executors.newFixedThreadPool(connCount);
        CountDownLatch createLatch = new CountDownLatch(connCount);
        final List<JdbcConnection> allList = new CopyOnWriteArrayList<>();

        // 并发创建
        for (int i = 0; i < connCount; i++) {
            executorService.execute(() -> {
                try {
                    Assertions.assertDoesNotThrow(() -> {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(super.defaultConnectionUrlRw);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allList.add(jdbcConnection);
                    });
                } finally {
                    createLatch.countDown();
                }
            });
        }

        // 等待创建完成
        Assertions.assertDoesNotThrow(() -> createLatch.await());

        // 判断
        TdsqlDirectDataSource ds1 = TdsqlDirectConnectionFactory.getDataSource(super.defaultConnectionUrlRw);
        Assertions.assertNotNull(ds1);

        TdsqlDirectConnectionManager cm1 = ds1.getConnectionManager();
        Assertions.assertNotNull(cm1);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm1 = cm1.getLiveConnectionMap();
        Assertions.assertNotNull(lcm1);
        Assertions.assertEquals(1, lcm1.size());
        Assertions.assertEquals(1, lcm1.keySet().size());

        Assertions.assertEquals(connCount, allList.size());

        TdsqlDirectCacheServer cs1 = ds1.getCacheServer();
        Assertions.assertNotNull(cs1);
        Assertions.assertTrue(cs1.getInitialCached());
        Assertions.assertFalse(cs1.getSurvived());
        Assertions.assertNotNull(cs1.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc1 = ds1.getScheduleServer().getMaster();
        Assertions.assertNotNull(mc1);
        Assertions.assertNotNull(mc1.getTdsqlHostInfo());
        Assertions.assertNotNull(mc1.getCount());
        Assertions.assertEquals(connCount, mc1.getCount().longValue());

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(connCount);
        for (JdbcConnection connection : allList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        TdsqlDirectDataSource ds2 = TdsqlDirectConnectionFactory.getDataSource(super.defaultConnectionUrlRw);
        Assertions.assertNotNull(ds2);

        TdsqlDirectConnectionManager cm2 = ds2.getConnectionManager();
        Assertions.assertNotNull(cm2);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm2 = cm2.getLiveConnectionMap();
        Assertions.assertNotNull(lcm2);
        Assertions.assertEquals(0, lcm2.size());
        Assertions.assertEquals(0, lcm2.keySet().size());

        for (JdbcConnection connection : allList) {
            try {
                Assertions.assertTrue(connection.isClosed());
            } catch (SQLException e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }
        allList.clear();

        TdsqlDirectCacheServer cs2 = ds2.getCacheServer();
        Assertions.assertNotNull(cs2);
        Assertions.assertTrue(cs2.getInitialCached());
        Assertions.assertFalse(cs2.getSurvived());
        Assertions.assertNotNull(cs2.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc2 = ds2.getScheduleServer().getMaster();
        Assertions.assertNotNull(mc2);
        Assertions.assertNotNull(mc2.getTdsqlHostInfo());
        Assertions.assertNotNull(mc2.getCount());
        Assertions.assertEquals(0L, mc2.getCount().longValue());

        executorService.shutdownNow();
    }

    /**
     * 创建500个连接 - 10个数据源 - 读写
     */
    @Test
    public void testCase03() {
        int dsCount = 10;
        int connPerDsCount = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(dsCount * connPerDsCount);

        // 组装10个连接串
        List<ConnectionUrl> urlList = new ArrayList<>(dsCount);
        for (int i = 0; i < dsCount; i++) {
            urlList.add(ConnectionUrl.getConnectionUrlInstance(DEFAULT_URL_RW + "&key=value" + i, super.defaultProperties));
        }

        // 并发创建，1个数据源50个连接
        List<JdbcConnection> allConnectionList = new CopyOnWriteArrayList<>();
        for (ConnectionUrl connectionUrl : urlList) {
            CountDownLatch createLatch = new CountDownLatch(connPerDsCount);
            for (int i = 0; i < connPerDsCount; i++) {
                executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                    try {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(connectionUrl);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allConnectionList.add(jdbcConnection);
                    } finally {
                        createLatch.countDown();
                    }
                }));
            }
            // 等待创建完成
            Assertions.assertDoesNotThrow(() -> createLatch.await());
        }

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(1, liveConnectionMap.size());
            Assertions.assertEquals(1, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(connPerDsCount, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNotNull(masterCounter);
            Assertions.assertNotNull(masterCounter.getTdsqlHostInfo());
            Assertions.assertNotNull(masterCounter.getCount());
            Assertions.assertEquals(connPerDsCount, masterCounter.getCount().longValue());
        }

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(dsCount * connPerDsCount);
        for (JdbcConnection connection : allConnectionList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(0, liveConnectionMap.size());
            Assertions.assertEquals(0, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(0, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNotNull(masterCounter);
            Assertions.assertNotNull(masterCounter.getTdsqlHostInfo());
            Assertions.assertNotNull(masterCounter.getCount());
            Assertions.assertEquals(0L, masterCounter.getCount().longValue());
        }

        executorService.shutdownNow();
    }

    /**
     * 创建1个连接 - 1个数据源 - 只读
     */
    @Test
    public void testCase04() {
        long perDsConnection = 1;
        int countDs = 1;

        Assertions.assertDoesNotThrow(() -> {
            try (JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(
                    super.defaultConnectionUrlRo)) {
                Assertions.assertNotNull(jdbcConnection);
                Assertions.assertTrue(jdbcConnection.isValid(3));

                TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(
                        super.defaultConnectionUrlRo);
                Assertions.assertNotNull(dataSource);

                TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
                Assertions.assertNotNull(connectionManager);

                Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
                Assertions.assertNotNull(liveConnectionMap);
                Assertions.assertEquals(perDsConnection, liveConnectionMap.size());
                Assertions.assertEquals(perDsConnection, liveConnectionMap.keySet().size());

                List<JdbcConnection> allList = new ArrayList<>();
                for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                    allList.addAll(connectionList);
                }
                Assertions.assertEquals(perDsConnection * countDs, allList.size());

                TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
                Assertions.assertNotNull(cacheServer);
                Assertions.assertTrue(cacheServer.getInitialCached());
                Assertions.assertFalse(cacheServer.getSurvived());
                Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
                Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

                TdsqlDirectScheduleServer scheduleServer = dataSource.getScheduleServer();

                TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();
                Assertions.assertNull(masterCounter);

                long totalCount = 0;
                for (TdsqlDirectConnectionCounter slaveCounter : scheduleServer.getSlaveSet()) {
                    totalCount += slaveCounter.getCount().longValue();
                }
                Assertions.assertEquals(perDsConnection, totalCount);
            }
        });
    }

    /**
     * 创建600个连接 - 1个数据源 - 只读 - LC
     */
    @Test
    public void testCase05() {
        int connCount = 600;
        ExecutorService executorService = Executors.newFixedThreadPool(connCount);
        CountDownLatch createLatch = new CountDownLatch(connCount);
        ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(
                DEFAULT_URL_RO + "&tdsqlLoadBalanceStrategy=lc", super.defaultProperties);
        final List<Connection> allList = new CopyOnWriteArrayList<>();

        // 并发创建
        for (int i = 0; i < connCount; i++) {
            executorService.execute(() -> {
                try {
                    Assertions.assertDoesNotThrow(() -> {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(connectionUrl);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allList.add(jdbcConnection);
                    });
                } finally {
                    createLatch.countDown();
                }
            });
        }

        // 等待创建完成
        Assertions.assertDoesNotThrow(() -> createLatch.await());

        // 判断
        Assertions.assertEquals(connCount, allList.size());

        TdsqlDirectDataSource ds1 = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
        Assertions.assertNotNull(ds1);

        TdsqlDirectConnectionManager cm1 = ds1.getConnectionManager();
        Assertions.assertNotNull(cm1);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm1 = cm1.getLiveConnectionMap();
        Assertions.assertNotNull(lcm1);
        Assertions.assertEquals(3, lcm1.size());
        Assertions.assertEquals(3, lcm1.keySet().size());

        TdsqlDirectCacheServer cs1 = ds1.getCacheServer();
        Assertions.assertNotNull(cs1);
        Assertions.assertTrue(cs1.getInitialCached());
        Assertions.assertFalse(cs1.getSurvived());
        Assertions.assertNotNull(cs1.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc1 = ds1.getScheduleServer().getMaster();
        Assertions.assertNull(mc1);

        for (TdsqlDirectConnectionCounter slaveCounter : ds1.getScheduleServer().getSlaveSet()) {
            Assertions.assertEquals(connCount / 3, slaveCounter.getCount().intValue());
        }

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(connCount);
        for (Connection connection : allList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        for (Connection connection : allList) {
            try {
                Assertions.assertTrue(connection.isClosed());
            } catch (SQLException e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }
        allList.clear();

        TdsqlDirectDataSource ds2 = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
        Assertions.assertNotNull(ds2);

        TdsqlDirectConnectionManager cm2 = ds2.getConnectionManager();
        Assertions.assertNotNull(cm2);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm2 = cm2.getLiveConnectionMap();
        Assertions.assertNotNull(lcm2);
        Assertions.assertEquals(0, lcm2.size());
        Assertions.assertEquals(0, lcm2.keySet().size());

        TdsqlDirectCacheServer cs2 = ds2.getCacheServer();
        Assertions.assertNotNull(cs2);
        Assertions.assertTrue(cs2.getInitialCached());
        Assertions.assertFalse(cs2.getSurvived());
        Assertions.assertNotNull(cs2.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc2 = ds2.getScheduleServer().getMaster();
        Assertions.assertNull(mc2);

        for (TdsqlDirectConnectionCounter slaveCounter : ds2.getScheduleServer().getSlaveSet()) {
            Assertions.assertEquals(0, slaveCounter.getCount().intValue());
        }

        executorService.shutdownNow();
    }

    /**
     * 创建600个连接 - 1个数据源 - 只读 - SED
     */
    @Test
    public void testCase06() {
        int connCount = 600;
        ExecutorService executorService = Executors.newFixedThreadPool(connCount);
        CountDownLatch createLatch = new CountDownLatch(connCount);
        ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(
                DEFAULT_URL_RO + "&tdsqlLoadBalanceStrategy=sed", super.defaultProperties);
        final List<Connection> allList = new CopyOnWriteArrayList<>();

        // 并发创建
        for (int i = 0; i < connCount; i++) {
            executorService.execute(() -> {
                try {
                    Assertions.assertDoesNotThrow(() -> {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(connectionUrl);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allList.add(jdbcConnection);
                    });
                } finally {
                    createLatch.countDown();
                }
            });
        }

        // 等待创建完成
        Assertions.assertDoesNotThrow(() -> createLatch.await());

        // 判断
        Assertions.assertEquals(connCount, allList.size());

        TdsqlDirectDataSource ds1 = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
        Assertions.assertNotNull(ds1);

        TdsqlDirectConnectionManager cm1 = ds1.getConnectionManager();
        Assertions.assertNotNull(cm1);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm1 = cm1.getLiveConnectionMap();
        Assertions.assertNotNull(lcm1);
        Assertions.assertEquals(3, lcm1.size());
        Assertions.assertEquals(3, lcm1.keySet().size());

        TdsqlDirectCacheServer cs1 = ds1.getCacheServer();
        Assertions.assertNotNull(cs1);
        Assertions.assertTrue(cs1.getInitialCached());
        Assertions.assertFalse(cs1.getSurvived());
        Assertions.assertNotNull(cs1.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs1.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc1 = ds1.getScheduleServer().getMaster();
        Assertions.assertNull(mc1);

        for (TdsqlDirectConnectionCounter slaveCounter : ds1.getScheduleServer().getSlaveSet()) {
            if (slaveCounter.getTdsqlHostInfo().getWeight() == 100) {
                Assertions.assertEquals(connCount / 5 * 2, slaveCounter.getCount().intValue());
            } else {
                Assertions.assertEquals(connCount / 5, slaveCounter.getCount().intValue());
            }
        }

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(connCount);
        for (Connection connection : allList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        for (Connection connection : allList) {
            try {
                Assertions.assertTrue(connection.isClosed());
            } catch (SQLException e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }
        allList.clear();

        TdsqlDirectDataSource ds2 = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
        Assertions.assertNotNull(ds2);

        TdsqlDirectConnectionManager cm2 = ds2.getConnectionManager();
        Assertions.assertNotNull(cm2);

        Map<TdsqlDirectHostInfo, List<JdbcConnection>> lcm2 = cm2.getLiveConnectionMap();
        Assertions.assertNotNull(lcm2);
        Assertions.assertEquals(0, lcm2.size());
        Assertions.assertEquals(0, lcm2.keySet().size());

        TdsqlDirectCacheServer cs2 = ds2.getCacheServer();
        Assertions.assertNotNull(cs2);
        Assertions.assertTrue(cs2.getInitialCached());
        Assertions.assertFalse(cs2.getSurvived());
        Assertions.assertNotNull(cs2.getLatestCachedTimeMillis());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getMasterTopologyInfo());
        Assertions.assertNotNull(cs2.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        TdsqlDirectConnectionCounter mc2 = ds2.getScheduleServer().getMaster();
        Assertions.assertNull(mc2);

        for (TdsqlDirectConnectionCounter slaveCounter : ds2.getScheduleServer().getSlaveSet()) {
            Assertions.assertEquals(0, slaveCounter.getCount().intValue());
        }

        executorService.shutdownNow();
    }

    /**
     * 创建600个连接 - 10个数据源 - 只读 - LC
     */
    @Test
    public void testCase07() {
        int dsCount = 10;
        int connPerDsCount = 60;
        ExecutorService executorService = Executors.newFixedThreadPool(dsCount * connPerDsCount);

        // 组装10个连接串
        List<ConnectionUrl> urlList = new ArrayList<>(dsCount);
        for (int i = 0; i < dsCount; i++) {
            urlList.add(ConnectionUrl.getConnectionUrlInstance(DEFAULT_URL_RO + "&tdsqlLoadBalanceStrategy=lc&key=value" + i, super.defaultProperties));
        }

        // 并发创建，1个数据源60个连接
        List<JdbcConnection> allConnectionList = new CopyOnWriteArrayList<>();
        for (ConnectionUrl connectionUrl : urlList) {
            CountDownLatch createLatch = new CountDownLatch(connPerDsCount);
            for (int i = 0; i < connPerDsCount; i++) {
                executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                    try {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(connectionUrl);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allConnectionList.add(jdbcConnection);
                    } finally {
                        createLatch.countDown();
                    }
                }));
            }
            // 等待创建完成
            Assertions.assertDoesNotThrow(() -> createLatch.await());
        }

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(3, liveConnectionMap.size());
            Assertions.assertEquals(3, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(connPerDsCount, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNull(masterCounter);

            for (TdsqlDirectConnectionCounter slaveCounter : dataSource.getScheduleServer().getSlaveSet()) {
                Assertions.assertEquals(connPerDsCount / 3, slaveCounter.getCount().intValue());
            }
        }

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(dsCount * connPerDsCount);
        for (JdbcConnection connection : allConnectionList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(0, liveConnectionMap.size());
            Assertions.assertEquals(0, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(0, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNull(masterCounter);

            for (TdsqlDirectConnectionCounter slaveCounter : dataSource.getScheduleServer().getSlaveSet()) {
                Assertions.assertEquals(0, slaveCounter.getCount().intValue());
            }
        }

        executorService.shutdownNow();
    }

    /**
     * 创建600个连接 - 10个数据源 - 只读 - SED
     */
    @Test
    public void testCase08() {
        int dsCount = 10;
        int connPerDsCount = 60;
        ExecutorService executorService = Executors.newFixedThreadPool(dsCount * connPerDsCount);

        // 组装10个连接串
        List<ConnectionUrl> urlList = new ArrayList<>(dsCount);
        for (int i = 0; i < dsCount; i++) {
            urlList.add(ConnectionUrl.getConnectionUrlInstance(DEFAULT_URL_RO + "&tdsqlLoadBalanceStrategy=sed&key=value" + i, super.defaultProperties));
        }

        // 并发创建，1个数据源60个连接
        List<JdbcConnection> allConnectionList = new CopyOnWriteArrayList<>();
        for (ConnectionUrl connectionUrl : urlList) {
            CountDownLatch createLatch = new CountDownLatch(connPerDsCount);
            for (int i = 0; i < connPerDsCount; i++) {
                executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                    try {
                        JdbcConnection jdbcConnection = TdsqlDirectConnectionFactory.createConnection(connectionUrl);
                        Assertions.assertNotNull(jdbcConnection);
                        Assertions.assertTrue(jdbcConnection.isValid(3));
                        allConnectionList.add(jdbcConnection);
                    } finally {
                        createLatch.countDown();
                    }
                }));
            }
            // 等待创建完成
            Assertions.assertDoesNotThrow(() -> createLatch.await());
        }

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(3, liveConnectionMap.size());
            Assertions.assertEquals(3, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(connPerDsCount, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNull(masterCounter);

            for (TdsqlDirectConnectionCounter slaveCounter : dataSource.getScheduleServer().getSlaveSet()) {
                if (slaveCounter.getTdsqlHostInfo().getWeight() == 100) {
                    Assertions.assertEquals(connPerDsCount / 5 * 2, slaveCounter.getCount().intValue());
                } else {
                    Assertions.assertEquals(connPerDsCount / 5, slaveCounter.getCount().intValue());
                }
            }
        }

        // 并发关闭
        CountDownLatch closeLatch = new CountDownLatch(dsCount * connPerDsCount);
        for (JdbcConnection connection : allConnectionList) {
            executorService.execute(() -> Assertions.assertDoesNotThrow(() -> {
                try {
                    connection.close();
                } finally {
                    closeLatch.countDown();
                }
            }));
        }

        // 等待关闭完成
        Assertions.assertDoesNotThrow(() -> closeLatch.await());

        // 判断
        for (ConnectionUrl connectionUrl : urlList) {
            TdsqlDirectDataSource dataSource = TdsqlDirectConnectionFactory.getDataSource(connectionUrl);
            Assertions.assertNotNull(dataSource);

            TdsqlDirectConnectionManager connectionManager = dataSource.getConnectionManager();
            Assertions.assertNotNull(connectionManager);

            Map<TdsqlDirectHostInfo, List<JdbcConnection>> liveConnectionMap = connectionManager.getLiveConnectionMap();
            Assertions.assertNotNull(liveConnectionMap);
            Assertions.assertEquals(0, liveConnectionMap.size());
            Assertions.assertEquals(0, liveConnectionMap.keySet().size());

            List<JdbcConnection> allList = new ArrayList<>();
            for (List<JdbcConnection> connectionList : liveConnectionMap.values()) {
                allList.addAll(connectionList);
            }
            Assertions.assertEquals(0, allList.size());

            TdsqlDirectCacheServer cacheServer = dataSource.getCacheServer();
            Assertions.assertNotNull(cacheServer);
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNotNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

            TdsqlDirectConnectionCounter masterCounter = dataSource.getScheduleServer().getMaster();
            Assertions.assertNull(masterCounter);

            for (TdsqlDirectConnectionCounter slaveCounter : dataSource.getScheduleServer().getSlaveSet()) {
                Assertions.assertEquals(0, slaveCounter.getCount().intValue());
            }
        }

        executorService.shutdownNow();
    }
}
