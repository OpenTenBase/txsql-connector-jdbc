package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

/**
 * <p>TDSQL专属，直连模式并发建连连接工厂类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConnectionFactory {

    private static boolean directModeCalled = false;
//    private static final LRUCache<String, TdsqlDirectDataSource> directDataSourceCache = new LRUCache<>(100);

    private static final Map<String, TdsqlDirectDataSource> directDataSourceMap = new ConcurrentHashMap<>();
//    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private static final Lock initTopoRefreshTaskMonitorLock = new ReentrantLock();
    private static final ScheduledThreadPoolExecutor topoRefreshTaskMonitor = new ScheduledThreadPoolExecutor(1, new TdsqlThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("TopoRefreshTaskMonitor").build());
    private static final boolean hasInitTopoRefreshTaskMonitor = false;

    private TdsqlDirectConnectionFactory() {
    }

    public static JdbcConnection createDirectConnection(ConnectionUrl connectionUrl) throws SQLException {
        initTopoRefreshTaskMonitor();

        directModeCalled = true;

        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        TdsqlDirectDataSource directDataSource = new TdsqlDirectDataSource(dataSourceUuid);

        int tryNum = 100;
        Lock readLock = null;
        while (tryNum != 0) {
            TdsqlDirectDataSource dataSource = directDataSourceMap.putIfAbsent(dataSourceUuid, directDataSource);
            if (dataSource == null) {
                readLock = directDataSource.getReadLock();
                readLock.lock();
                logInfo("create new datasource：" + connectionUrl.safeToString());
                directDataSource.initialize(connectionUrl);
                break;
            } else {
                readLock = dataSource.getReadLock();
                readLock.lock();
                if (dataSource.getActiveState()) {
                    directDataSource = dataSource;
                    break;
                }

                readLock.unlock();
            }
            tryNum--;
        }
        if (tryNum == 0) {
            throw TdsqlExceptionFactory.logException(dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                    "init directDataSource failed!");
        }

        try {
            if (directDataSource.waitForFirstFinished()) {
                return directDataSource.getConnectionManager().createNewConnection();
            }
        } finally {
            readLock.unlock();
        }
        return null;
    }

    private static void initTopoRefreshTaskMonitor() {
        if (hasInitTopoRefreshTaskMonitor) {
            return;
        }
        initTopoRefreshTaskMonitorLock.lock();
        try {
            if (hasInitTopoRefreshTaskMonitor) {
                return;
            }
            topoRefreshTaskMonitor.scheduleWithFixedDelay(new TdsqlDirectTopoRefreshTaskMonitor(), 0L, 1000, TimeUnit.MILLISECONDS);
        } finally {
            initTopoRefreshTaskMonitorLock.unlock();
        }
    }

//    public static JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
//        directModeCalled = true;
//
//        // 根据URL配置，生成数据源UUID，为16进制32位MD5哈希字符串
//        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
//        TdsqlDirectDataSource directDataSource;
//
//        rwLock.readLock().lock();
//        directDataSource = directDataSourceCache.get(dataSourceUuid);
//        if (directDataSource == null) {
//            rwLock.readLock().unlock();
//            rwLock.writeLock().lock();
//            try {
//                // 再次检查，可能已被另一个线程缓存
//                directDataSource = directDataSourceCache.get(dataSourceUuid);
//                if (directDataSource == null) {
//                    directDataSource = new TdsqlDirectDataSource(dataSourceUuid);
//                    directDataSource.initialize(connectionUrl);
//                    directDataSourceCache.put(dataSourceUuid, directDataSource);
//                    logInfo(Messages.getString("TdsqlDirectDataSourceMessage.CreateNewDataSource",
//                            new Object[]{dataSourceUuid}));
//                }
//                rwLock.readLock().lock();
//            } finally {
//                rwLock.writeLock().unlock();
//            }
//        }
//
//        try {
//            if (directDataSource.getCacheServer().waitForFirstFinished()) {
//                return directDataSource.getConnectionManager().createNewConnection();
//            }
//        } finally {
//            rwLock.readLock().unlock();
//        }
//
//        return null;
//    }

    /**
     * 标识是否有直连模式调用
     *
     * @return 有直连模式调用返回 {@code true}，否则返回 {@code false}
     */
    public static boolean directModeCalled() {
        return directModeCalled;
    }

    /**
     * 根据URL配置，获取数据源
     *
     * @param connectionUrl {@link ConnectionUrl}
     * @return 如果已成功创建则返回该数据源，否则返回 {@code null}
     */
    public static TdsqlDirectDataSource getDataSource(ConnectionUrl connectionUrl) {
        // 根据URL配置，生成数据源UUID，为16进制32位MD5哈希字符串
        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        return directDataSourceMap.getOrDefault(dataSourceUuid, null);
    }

    private static class TdsqlDirectTopoRefreshTaskMonitor implements Runnable {

        @Override
        public void run() {
            try {
                for(Map.Entry<String, TdsqlDirectDataSource> dataSourceEntry : TdsqlDirectConnectionFactory.directDataSourceMap.entrySet()) {
                    TdsqlDirectDataSource dataSource = dataSourceEntry.getValue();
                    if (!dataSource.shouldBeClosed()) {
                        continue;
                    }
                    Lock writeLock = dataSource.getWriteLock();
                    writeLock.lock();
                    try {
                        if (!dataSource.shouldBeClosed()) {
                            writeLock.unlock();
                            continue;
                        }
                        logInfo("remove data source:" + dataSource.getDataSourceUuid());
                        TdsqlDirectConnectionFactory.directDataSourceMap.remove(dataSourceEntry.getKey());
                        dataSource.setActiveState(false);
                    } finally {
                        writeLock.unlock();
                    }
                    dataSource.close();
                }
            } catch (Throwable e) {
                logError(e.getMessage(), e);
            }
        }
    }
}
