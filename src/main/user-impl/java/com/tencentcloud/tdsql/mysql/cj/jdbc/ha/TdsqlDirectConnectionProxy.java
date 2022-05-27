package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RW;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.convert;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TDSQLNoBackendInstanceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
import java.sql.SQLException;
import java.util.List;
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

        TdsqlDirectReadWriteMode readWriteMode = convert(topoServer.getTdsqlReadWriteMode());
        List<DataSetInfo> masters = DataSetCache.getInstance().getMasters();
        List<DataSetInfo> slaves = DataSetCache.getInstance().getSlaves();
        if (RW.equals(readWriteMode) && masters.isEmpty()) {
            throw new TDSQLNoBackendInstanceException("No master instance found, master size: 0");
        }
        if (RO.equals(readWriteMode) && slaves.isEmpty()) {
            throw new TDSQLNoBackendInstanceException("No slave instance found");
        }
        TdsqlDirectLoggerFactory.logDebug("now masters: " + masters);

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
