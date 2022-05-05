package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<String, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();

    private TdsqlDirectConnectionManager() {
    }

    public static TdsqlDirectConnectionManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized JdbcConnection pickNewConnection(HostInfo hostInfo) throws SQLException {
        JdbcConnection connection = ConnectionImpl.getInstance(hostInfo);

        String connKey = hostInfo.getHostPortPair();
        List<JdbcConnection> holderList = connectionHolder.getOrDefault(connKey, new ArrayList<>());
        Iterator<JdbcConnection> iterator = holderList.iterator();
        while (iterator.hasNext()) {
            JdbcConnection conn = iterator.next();
            if (conn == null || conn.isClosed()) {
                iterator.remove();
            }
        }
        holderList.add(connection);
        connectionHolder.put(connKey, holderList);

        return connection;
    }

    public synchronized ConcurrentHashMap<String, List<JdbcConnection>> getAllConnection() {
        return connectionHolder;
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
