package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import com.tencentcloud.tdsql.mysql.cj.util.LRUCache;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

/**
 * <p>TDSQL专属，直连模式并发建连连接工厂类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConnectionFactory {

    private static boolean directModeCalled = false;
    private static final LRUCache<String, TdsqlDirectDataSource> directDataSourceCache = new LRUCache<>(100);

    private static final Map<String, TdsqlDirectDataSource> directDataSourceMap = new ConcurrentHashMap<>();
    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private TdsqlDirectConnectionFactory() {
    }

    public static JdbcConnection createDirectConnection(ConnectionUrl connectionUrl) throws SQLException {
        directModeCalled = true;

        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        TdsqlDirectDataSource directDataSource = new TdsqlDirectDataSource(dataSourceUuid);
        TdsqlDirectDataSource dataSource = directDataSourceMap.putIfAbsent(dataSourceUuid, directDataSource);
        if (dataSource == null) {
            logInfo("新增数据源：" + connectionUrl.toString());
            directDataSource.initialize(connectionUrl);
        } else {
            directDataSource = dataSource;
        }

        if (directDataSource.waitForFirstFinished()) {
            return directDataSource.getConnectionManager().createNewConnection();
        }
        return null;
    }

    public static JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
        directModeCalled = true;

        // 根据URL配置，生成数据源UUID，为16进制32位MD5哈希字符串
        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        TdsqlDirectDataSource directDataSource;

        rwLock.readLock().lock();
        directDataSource = directDataSourceCache.get(dataSourceUuid);
        if (directDataSource == null) {
            rwLock.readLock().unlock();
            rwLock.writeLock().lock();
            try {
                // 再次检查，可能已被另一个线程缓存
                directDataSource = directDataSourceCache.get(dataSourceUuid);
                if (directDataSource == null) {
                    directDataSource = new TdsqlDirectDataSource(dataSourceUuid);
                    directDataSource.initialize(connectionUrl);
                    directDataSourceCache.put(dataSourceUuid, directDataSource);
                    logInfo(Messages.getString("TdsqlDirectDataSourceMessage.CreateNewDataSource",
                            new Object[]{dataSourceUuid}));
                }
                rwLock.readLock().lock();
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        try {
            if (directDataSource.getCacheServer().waitForFirstFinished()) {
                return directDataSource.getConnectionManager().createNewConnection();
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return null;
    }

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
}
