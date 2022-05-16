package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();

    private TdsqlDirectConnectionManager() {
    }

    public static TdsqlDirectConnectionManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized JdbcConnection pickNewConnection(TdsqlLoadBalanceStrategy balancer) throws SQLException {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = topoServer.getScheduleQueue();

        TdsqlHostInfo tdsqlHostInfo = balancer.choice(scheduleQueue);
        JdbcConnection connection = ConnectionImpl.getInstance(tdsqlHostInfo);

        List<JdbcConnection> holderList = connectionHolder.getOrDefault(tdsqlHostInfo, new ArrayList<>());
        holderList.add(connection);
        connectionHolder.put(tdsqlHostInfo, holderList);
        scheduleQueue.incrementAndGet(tdsqlHostInfo);
        return connection;
    }

    public ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> getAllConnection() {
        return connectionHolder;
    }

    public void close(List<String> toCloseList) {
        if (toCloseList == null || toCloseList.isEmpty()) {
            TdsqlDirectLoggerFactory.logError("To close list is empty, close operation ignore!");
            return;
        }
        try {
            for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : connectionHolder.entrySet()) {
                TdsqlHostInfo removeKey = entry.getKey();
                if (toCloseList.contains(removeKey.getHostPortPair())) {
                    connectionHolder.remove(removeKey);
                    for (JdbcConnection jdbcConnection : entry.getValue()) {
                        if (jdbcConnection != null && !jdbcConnection.isClosed()) {
                            jdbcConnection.close();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            TdsqlDirectLoggerFactory.logError(e.getMessage(), e);
        }
    }

    public void closeAll() {
        try {
            for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : connectionHolder.entrySet()) {
                for (JdbcConnection jdbcConnection : entry.getValue()) {
                    if (jdbcConnection != null && !jdbcConnection.isClosed()) {
                        jdbcConnection.close();
                    }
                }
            }
            connectionHolder.clear();
        } catch (SQLException e) {
            TdsqlDirectLoggerFactory.logError(e.getMessage(), e);
        }
    }

    public List<JdbcConnection> getConnectionList(TdsqlHostInfo tdsqlHostInfo) {
        return connectionHolder.getOrDefault(tdsqlHostInfo, Collections.emptyList());
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
