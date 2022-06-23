package com.tencentcloud.tdsql.mysql.cj.jdbc;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_CLOSE_CONNECTION_TIMEOUT_MILLISECONDS;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.AbstractTdsqlCaughtRunnable;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.SynchronousExecutor;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();
    private final Executor netTimeoutExecutor = new SynchronousExecutor();

    private TdsqlDirectConnectionManager() {
        initializeCompensator();
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

    public synchronized void close(List<String> toCloseList) {
        if (toCloseList == null || toCloseList.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("To close list is empty, close operation ignore!");
            return;
        }

        Iterator<Entry<TdsqlHostInfo, List<JdbcConnection>>> entryIterator = connectionHolder.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Entry<TdsqlHostInfo, List<JdbcConnection>> entry = entryIterator.next();
            String holdHostPortPair = entry.getKey().getHostPortPair();
            if (toCloseList.contains(holdHostPortPair)) {
                TdsqlDirectLoggerFactory.logDebug("Start close [" + holdHostPortPair + "]'s connections!");
                try {
                    Iterator<JdbcConnection> connIterator = entry.getValue().iterator();
                    while (connIterator.hasNext()) {
                        try (JdbcConnection connection = connIterator.next()) {
                            connection.setNetworkTimeout(netTimeoutExecutor,
                                    TDSQL_CLOSE_CONNECTION_TIMEOUT_MILLISECONDS);
                        } catch (SQLException e) {
                            TdsqlDirectLoggerFactory.logError("Set close connection timeout error: " + e.getMessage(),
                                    e);
                        } finally {
                            connIterator.remove();
                        }
                    }
                } finally {
                    entryIterator.remove();
                }
                TdsqlDirectLoggerFactory.logDebug("Finish close [" + holdHostPortPair + "]'s connections!");
            }
        }
    }

    public synchronized void closeAll() {
        if (connectionHolder.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("To close list is empty, close all operation ignore!");
            return;
        }

        List<String> toCloseList = connectionHolder.keySet().stream().map(HostInfo::getHostPortPair)
                .collect(Collectors.toList());
        TdsqlDirectLoggerFactory.logDebug("Start close all connections! Contains: [" + toCloseList + "]");
        this.close(toCloseList);
        TdsqlDirectLoggerFactory.logDebug("Finish close all connections!");
    }

    private void initializeCompensator() {
        ScheduledThreadPoolExecutor compensator = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Compensator-pool-").build());
        compensator.scheduleWithFixedDelay(new CompensatorTask(), 0L, 1L, TimeUnit.SECONDS);
    }

    private static class CompensatorTask extends AbstractTdsqlCaughtRunnable {

        @Override
        public void caughtAndRun() {
            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = TdsqlDirectConnectionManager.getInstance()
                    .getAllConnection();
            for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : connectionHolder.entrySet()) {
                TdsqlHostInfo tdsqlHostInfo = entry.getKey();
                int realCount = entry.getValue().size();
                if (!scheduleQueue.containsKey(tdsqlHostInfo)) {
                    return;
                }
                long currentCount = scheduleQueue.get(tdsqlHostInfo);
                if (realCount != currentCount) {
                    scheduleQueue.put(tdsqlHostInfo, realCount);
                }
            }
        }
    }

    public List<JdbcConnection> getConnectionList(TdsqlHostInfo tdsqlHostInfo) {
        return connectionHolder.getOrDefault(tdsqlHostInfo, Collections.emptyList());
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
