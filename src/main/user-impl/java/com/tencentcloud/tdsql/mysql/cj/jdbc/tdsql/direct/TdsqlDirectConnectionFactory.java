package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectDataSourceCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.log.Log;
import com.tencentcloud.tdsql.mysql.cj.log.LogFactory;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionFactory {
    public static boolean directMode = false;
    private TdsqlDirectConnectionFactory() {
    }

    public synchronized JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
        directMode = true;
//        TdsqlLoggerFactory.logInfo("---------------------LogInfo-------------------");
//        TdsqlLoggerFactory.logError("---------------------Logerr----------------------");
//        TdsqlLoggerFactory.logDebug("---------------------Logdebug----------------------");
//        TdsqlLoggerFactory.logFatal("---------------------Logfatal----------------------");
        //针对每一个ConnectionUrl，新建一个直连信息类TdsqlDirectInfo
        TdsqlDirectInfo tdsqlDirectInfo = this.validateConnectionAttributes(connectionUrl);
        //根据TdsqlDirectInfo初始化直连多数据源信息记录类
        TdsqlDirectDataSourceCounter.getInstance().initialize(tdsqlDirectInfo);

        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().
                getTdsqlDirectInfo(tdsqlDirectInfo.getDatasourceUuid()).getTopoServer();
        ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
        topoServer.initialize(connectionUrl);
        refreshLock.readLock().lock();
        JdbcConnection newConnection;

        try {
            newConnection = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(tdsqlDirectInfo.getDatasourceUuid()).
                    getTdsqlDirectConnectionManager().createNewConnection(connectionUrl);
        } finally {
            refreshLock.readLock().unlock();
        }
        return newConnection;
    }

    private TdsqlDirectInfo validateConnectionAttributes(ConnectionUrl connectionUrl){
        TdsqlDirectInfo tdsqlDirectInfo = new TdsqlDirectInfo();
        tdsqlDirectInfo.setDatasourceUuid(connectionUrl);
        return tdsqlDirectInfo;
    }

    public void closeConnection(JdbcConnection jdbcConnection, TdsqlHostInfo tdsqlHostInfo) {
        TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(tdsqlHostInfo.getOwnerUuid()).getTdsqlDirectConnectionManager().
                getConnectionList(tdsqlHostInfo).removeIf(cachedConnection -> cachedConnection.equals(jdbcConnection));
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectDataSourceCounter.getInstance().
                getTdsqlDirectInfo(tdsqlHostInfo.getOwnerUuid()).getTopoServer().getScheduleQueue();
        if (scheduleQueue.containsKey(tdsqlHostInfo)) {
            scheduleQueue.decrementAndGet(tdsqlHostInfo);
        }
    }

    public static TdsqlDirectConnectionFactory getInstance() {
        return TdsqlDirectConnectionFactory.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {
        private static final TdsqlDirectConnectionFactory INSTANCE = new TdsqlDirectConnectionFactory();
    }

}
