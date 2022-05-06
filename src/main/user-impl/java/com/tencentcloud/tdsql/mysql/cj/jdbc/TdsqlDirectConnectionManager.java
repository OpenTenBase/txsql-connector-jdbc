package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder;

    private TdsqlDirectConnectionManager() {
        this.connectionHolder = new ConcurrentHashMap<>();
        initialize();
    }

    public static TdsqlDirectConnectionManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized JdbcConnection pickNewConnection(HostInfo hostInfo) throws SQLException {
        JdbcConnection connection = ConnectionImpl.getInstance(hostInfo);
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue = topoServer.getScheduleQueue();

        TdsqlHostInfo tdsqlHostInfo = new TdsqlHostInfo(hostInfo);
        List<JdbcConnection> holderList = connectionHolder.getOrDefault(tdsqlHostInfo, new ArrayList<>());
        holderList.add(connection);
        connectionHolder.put(tdsqlHostInfo, holderList);

        topoServer.lock.lock();
        try {
            Long adder = scheduleQueue.getOrDefault(tdsqlHostInfo, 0L);
            scheduleQueue.put(tdsqlHostInfo, ++adder);
        } finally {
            topoServer.lock.unlock();
        }
        return connection;
    }

    private void initialize() {
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
                            ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue = topoServer.getScheduleQueue();
                            topoServer.lock.lock();
                            try {
                                Long adder = scheduleQueue.getOrDefault(tdsqlHostInfo, 0L);
                                scheduleQueue.put(tdsqlHostInfo, --adder);
                            } finally {
                                topoServer.lock.unlock();
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, 0L, 1L, TimeUnit.SECONDS);
    }

    public synchronized ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> getAllConnection() {
        return connectionHolder;
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
