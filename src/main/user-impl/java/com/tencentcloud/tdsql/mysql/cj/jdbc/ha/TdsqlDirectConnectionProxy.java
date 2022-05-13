package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TDSQLNoBackendInstanceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionProxy {

    public static boolean directMode = false;

    private TdsqlDirectConnectionProxy() {
    }

    public static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        directMode = true;
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
        topoServer.initialize(connectionUrl);

        TdsqlDirectReadWriteMode readWriteMode = TdsqlDirectReadWriteMode.convert(topoServer.getTdsqlReadWriteMode());
        TdsqlDirectLoggerFactory.getLogger().logDebug("now masters: " + DataSetUtil.dataSetList2String(DataSetCache.getInstance().getMasters()));
        if (TdsqlDirectReadWriteMode.RW.equals(readWriteMode) && DataSetCache.getInstance().noMaster()) {
            throw new TDSQLNoBackendInstanceException("No master instance found");
        }
        if (TdsqlDirectReadWriteMode.RO.equals(readWriteMode) && DataSetCache.getInstance().getSlaves().size() == 0) {
            throw new TDSQLNoBackendInstanceException("No slave instance found");
        }

        refreshLock.readLock().lock();
        JdbcConnection newConnection;
        try {
            newConnection = TdsqlDirectConnectionManager.getInstance()
                    .pickNewConnection(new TdsqlDirectLoadBalanceStrategy());
        } finally {
            refreshLock.readLock().unlock();
        }
        return newConnection;
    }

    public static void closeProxyInstance(JdbcConnection jdbcConnection, HostInfo hostInfo) {
        TdsqlHostInfo tdsqlHostInfo = new TdsqlHostInfo(hostInfo);
        TdsqlDirectConnectionManager.getInstance().getConnectionList(tdsqlHostInfo)
                .removeIf(cachedConnection -> cachedConnection.equals(jdbcConnection));
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
        if (scheduleQueue.containsKey(tdsqlHostInfo)) {
            scheduleQueue.decrementAndGet(tdsqlHostInfo);
        }
    }
}
