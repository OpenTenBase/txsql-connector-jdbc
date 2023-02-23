package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectDataSourceCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>TDSQL-NySQL专属的，在建立直连的数据库连接时，实现了静态读写分离的处理类</p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionFactory {

    /**
     * 标识是否启用TDSQL-MySQL专属的直连模式
     * 目的是在关闭连接时，有针对性的进行额外的处理，
     * 这里额外的处理主要的目的是：关闭连接的同时，对全局连接计数器进行更新
     */
    public static boolean tdsqlDirectMode = false;

    private TdsqlDirectConnectionFactory() {
    }

    public synchronized JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
        tdsqlDirectMode = true;
        // 针对每一个ConnectionUrl，新建一个直连信息类TdsqlDirectInfo
        TdsqlDirectInfo tdsqlDirectInfo = this.validateConnectionAttributes(connectionUrl);
        // 根据TdsqlDirectInfo初始化直连多数据源信息记录类
        TdsqlDirectDataSourceCounter.getInstance().initialize(tdsqlDirectInfo);

        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().
                getTdsqlDirectInfo(tdsqlDirectInfo.getDatasourceUuid()).getTopoServer();
        ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
        topoServer.initialize(connectionUrl);
        refreshLock.readLock().lock();
        JdbcConnection newConnection;

        try {
            newConnection = TdsqlDirectDataSourceCounter.getInstance()
                    .getTdsqlDirectInfo(tdsqlDirectInfo.getDatasourceUuid()).
                    getTdsqlDirectConnectionManager().createNewConnection(connectionUrl);
        } finally {
            refreshLock.readLock().unlock();
        }
        return newConnection;
    }

    private TdsqlDirectInfo validateConnectionAttributes(ConnectionUrl connectionUrl) {
        TdsqlDirectInfo tdsqlDirectInfo = new TdsqlDirectInfo();
        tdsqlDirectInfo.setDatasourceUuid(connectionUrl);
        return tdsqlDirectInfo;
    }

    public synchronized void closeConnection(JdbcConnection jdbcConnection, TdsqlHostInfo tdsqlHostInfo) {
        if (Objects.equals(TdsqlConnectionMode.DIRECT, tdsqlHostInfo.getConnectionMode())) {
            logInfo("[" + tdsqlHostInfo.getOwnerUuid() + "] Direct Mode close method called. ["
                    + tdsqlHostInfo.getHostPortPair() + "]");
            TdsqlDirectInfo tdsqlDirectInfo = TdsqlDirectDataSourceCounter.getInstance()
                    .getTdsqlDirectInfo(tdsqlHostInfo.getOwnerUuid());
            tdsqlDirectInfo.getTdsqlDirectConnectionManager().getConnectionList(tdsqlHostInfo)
                    .removeIf(cachedConnection -> cachedConnection.equals(jdbcConnection));
            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = tdsqlDirectInfo.getTopoServer().getScheduleQueue();
            if (scheduleQueue.containsKey(tdsqlHostInfo)) {
                scheduleQueue.decrementAndGet(tdsqlHostInfo);
            }
        }
    }

    public static TdsqlDirectConnectionFactory getInstance() {
        return TdsqlDirectConnectionFactory.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionFactory INSTANCE = new TdsqlDirectConnectionFactory();
    }

}
