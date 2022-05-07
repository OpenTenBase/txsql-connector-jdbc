package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

        HostInfo hostInfo = balancer.choice(scheduleQueue);
        System.out.println("hostInfo = " + hostInfo);
        JdbcConnection connection = ConnectionImpl.getInstance(hostInfo);
        TdsqlHostInfo tdsqlHostInfo = new TdsqlHostInfo(hostInfo);

        List<JdbcConnection> holderList = connectionHolder.getOrDefault(tdsqlHostInfo, new ArrayList<>());
        holderList.add(connection);
        connectionHolder.put(tdsqlHostInfo, holderList);
        scheduleQueue.incrementAndGet(tdsqlHostInfo);
        return connection;
    }

    private void initializeRecycler() {
        ScheduledThreadPoolExecutor recycler = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setNameFormat("Recycle-pool-").build());
        recycler.scheduleAtFixedRate(() -> {
            try {
                for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : getAllConnection().entrySet()) {
                    TdsqlHostInfo tdsqlHostInfo = entry.getKey();
                    Iterator<JdbcConnection> iterator = entry.getValue().iterator();
                    while (iterator.hasNext()) {
                        JdbcConnection connection = iterator.next();
                        if (connection == null || connection.isClosed()) {
                            iterator.remove();

                            TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
                            ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
                            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = topoServer.getScheduleQueue();
                            refreshLock.writeLock().lock();
                            try {
                                if (scheduleQueue.containsKey(tdsqlHostInfo)) {
                                    scheduleQueue.decrementAndGet(tdsqlHostInfo);
                                }
                            } finally {
                                refreshLock.writeLock().unlock();
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, 0L, 1L, TimeUnit.SECONDS);
    }

    public ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> getAllConnection() {
        return connectionHolder;
    }

    public void close(List<String> toCloseList) {
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
            e.printStackTrace();
        } finally {
            for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : connectionHolder.entrySet()) {
                System.out.println(
                        "ConnectionHolder,Host:Size = " + entry.getKey().getHostPortPair() + ":" + entry.getValue()
                                .size());
            }
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
            e.printStackTrace();
        }
    }

    public List<JdbcConnection> getConnectionList(TdsqlHostInfo tdsqlHostInfo) {
        return connectionHolder.getOrDefault(tdsqlHostInfo, Collections.emptyList());
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
