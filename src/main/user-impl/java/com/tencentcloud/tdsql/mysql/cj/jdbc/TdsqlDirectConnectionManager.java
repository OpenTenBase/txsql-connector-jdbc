package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.AbstractTdsqlCaughtRunnable;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.SynchronousExecutor;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();
    private ThreadPoolExecutor recycler;

    private TdsqlDirectConnectionManager() {
        initializeCompensator();
        initializeRecycler();
    }

    public static TdsqlDirectConnectionManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized JdbcConnection pickNewConnection(TdsqlLoadBalanceStrategy balancer) throws SQLException {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = topoServer.getScheduleQueue();

        TdsqlHostInfo tdsqlHostInfo = balancer.choice(scheduleQueue);
        JdbcConnection connection = ConnectionImpl.getInstance(tdsqlHostInfo);

        List<JdbcConnection> holderList = connectionHolder.getOrDefault(tdsqlHostInfo,
                new CopyOnWriteArrayList<>());
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
        this.recycler.submit(new RecyclerTask(toCloseList));
    }

    private void initializeCompensator() {
        ScheduledThreadPoolExecutor compensator = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Compensator-pool-").build());
        compensator.scheduleWithFixedDelay(new CompensatorTask(), 0L, 1L, TimeUnit.SECONDS);
    }

    private void initializeRecycler() {
        this.recycler = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Recycler-pool-").build(),
                new AbortPolicy());
        this.recycler.allowCoreThreadTimeOut(true);
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

    private static class RecyclerTask extends AbstractTdsqlCaughtRunnable {

        private final List<String> recycleList;
        private final Executor netTimeoutExecutor = new SynchronousExecutor();

        private RecyclerTask(List<String> recycleList) {
            this.recycleList = recycleList;
        }

        @Override
        public void caughtAndRun() {
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> allConnection =
                    TdsqlDirectConnectionManager.getInstance().getAllConnection();
            Iterator<Entry<TdsqlHostInfo, List<JdbcConnection>>> entryIterator = allConnection.entrySet()
                    .iterator();
            while (entryIterator.hasNext()) {
                Entry<TdsqlHostInfo, List<JdbcConnection>> entry = entryIterator.next();
                String holdHostPortPair = entry.getKey().getHostPortPair();
                if (recycleList.contains(holdHostPortPair)) {
                    TdsqlDirectLoggerFactory.logDebug("Start close [" + holdHostPortPair + "]'s connections!");
                    for (JdbcConnection jdbcConnection : entry.getValue()) {
                        ConnectionImpl connection = (ConnectionImpl) jdbcConnection;
                        if (connection != null && !connection.isClosed()) {
                            try {
                                connection.setNetworkTimeout(netTimeoutExecutor, TdsqlDirectTopoServer.getInstance()
                                        .getTdsqlDirectCloseConnTimeoutMillis());
                            } catch (Exception e) {
                                // ignore
                            } finally {
                                try {
                                    connection.close();
                                } catch (Exception e) {
                                    TdsqlDirectLoggerFactory.logError(
                                            "Closing [" + holdHostPortPair + "] connection failed!");
                                }
                            }
                        }
                    }
                    entryIterator.remove();
                    TdsqlDirectLoggerFactory.logDebug("Finish close [" + holdHostPortPair + "]'s connections!");
                } else {
                    TdsqlDirectLoggerFactory.logDebug("To closes not in connection holder! NOOP!");
                }
            }
        }
    }

    public List<JdbcConnection> getConnectionList(TdsqlHostInfo tdsqlHostInfo) {
        return connectionHolder.getOrDefault(tdsqlHostInfo, new CopyOnWriteArrayList<>());
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
