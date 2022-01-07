package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.MysqlConnection;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConnectionManager {

    public Set<String> blackList = new LinkedHashSet<String>();
    public static final ConcurrentHashMap<String, Integer> HOST_CONNECTION_COUNT_MAP = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Connection> heartbeatMap = new ConcurrentHashMap<String, Connection>();
    private final Map<String, List<Connection>> hostConnectionMap = new ConcurrentHashMap<String, List<Connection>>();

    private final Map<String, Properties> propMap = new HashMap<String, Properties>();
    private Properties props = null;
    private static final ConnectionManager CONNECTION_MANAGER = new ConnectionManager();
    private final Set<String> hosts = new LinkedHashSet<String>();
    private final Map<String, Integer> weightFactor = new HashMap<String, Integer>();
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);

    private int haLoadBalanceMaximumErrorRetries = 0;
    private int haLoadBalanceHeartbeatIntervalTime = 3000;
    private int haLoadBalanceBlacklistTimeout = 5000;
    private boolean haLoadBalanceHeartbeatMonitor = true;

    public static ConnectionManager getInstance() {
        return CONNECTION_MANAGER;
    }

    public Map<String, Properties> getPropMap() {
        return this.propMap;
    }

    public void setProps(Properties p) {
        if (this.props == null && p != null) {
            this.props = p;
        }
    }

    public void addConnection(HostInfo hostInfo, final String host, Connection conn) {
        List<Connection> l;
        Map<String, List<Connection>> map = this.hostConnectionMap;
        synchronized (map) {
            l = this.hostConnectionMap.containsKey(host) ? this.hostConnectionMap.get(host)
                    : new ArrayList<Connection>();
            for (Connection c : l) {
                try {
                    if (c.isClosed()) {
                        l.remove(c);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            l.add(conn);
            this.hostConnectionMap.put(host, l);
        }
        final String ht = host;
        Thread haLoadBalanceHeartbeatMonitor = new Thread() {
            @Override
            public void run() {
                int i = 0;
                Connection newConn = null;
                while (i++ < 5) {
                    try {
                        Map<String, Connection> map = ConnectionManager.this.heartbeatMap;
                        synchronized (map) {
                            if (!ConnectionManager.this.heartbeatMap.containsKey(ht)) {
                                Properties prop = ((com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl) (ConnectionManager.this.hostConnectionMap.get(
                                        ht)).get(0)).getProperties();
                                ConnectionManager.getInstance().setProps(prop);
                                newConn = ConnectionManager.this.copyConn(hostInfo, ht);
                                ConnectionManager.this.heartbeatMap.put(ht, newConn);
                                break;
                            }
                        }
                    } catch (Exception sqlEx) {
                        ConnectionManager.this.log(newConn,
                                "haLoadBalanceHeartbeatMonitor SQLException " + sqlEx.getMessage());
                    }
                }
                if (newConn != null) {
                    java.sql.Statement stmt;
                    boolean flag = true;
                    int attempts = 0;
                    while (flag) {
                        try {
                            Thread.sleep(haLoadBalanceHeartbeatIntervalTime);
                            if (newConn == null || newConn.isClosed()) {
                                continue;
                            }
                            stmt = newConn.createStatement();
                            stmt.execute("select 1");
                            stmt.close();
                            List<Connection> scons = ConnectionManager.this.hostConnectionMap.get(host);
                            int k = 0;
                            List<Connection> list = scons;
                            synchronized (list) {
                                Iterator<Connection> iterator = scons.iterator();
                                while (iterator.hasNext()) {
                                    Connection scon = iterator.next();
                                    if (!scon.isClosed()) {
                                        continue;
                                    }
                                    iterator.remove();
                                    ++k;
                                }
                                if (k == scons.size()) {
                                    break;
                                }
                                continue;
                            }
                        } catch (Exception e) {
                            if (++attempts < haLoadBalanceMaximumErrorRetries) {
                                continue;
                            }
                            ConnectionManager.this.blackList.add(ht);
                            ConnectionManager.HOST_CONNECTION_COUNT_MAP.remove(ht);
                            ConnectionManager.this.log(newConn, "add ip [" + ht + "] to blacklist");
                            ConnectionManager.this.log(newConn, "current black list [" + ConnectionManager.this.blackList + "]");
                            BlackListTask blackListTask = new BlackListTask(hostInfo, ht, haLoadBalanceBlacklistTimeout);
                            ConnectionManager.this.executor.schedule(blackListTask, haLoadBalanceBlacklistTimeout,
                                    TimeUnit.MILLISECONDS);
                            ConnectionManager.this.heartbeatMap.remove(ht);
                        }
                        ConnectionManager.this.hostConnectionMap.remove(host);
                        flag = false;
                        attempts = 0;
                    }
                }
            }
        };
        if (this.props == null) {
            this.props = ((com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl) this.hostConnectionMap.get(ht)
                    .get(0)).getProperties();
        }
        if (this.haLoadBalanceHeartbeatMonitor) {
            haLoadBalanceHeartbeatMonitor.setDaemon(true);
            haLoadBalanceHeartbeatMonitor.start();
        }
    }

    private Connection copyConn(HostInfo hostInfo, String host) throws SQLException {
        return ConnectionImpl.getInstance(hostInfo);
        /*Properties prop = this.props;
        if (this.props == null) {
            return null;
        }
        int connectionTimeout = Integer.parseInt(prop.getProperty("connectionTimeout", "1200"));
        int socketTimeout = Integer.parseInt(prop.getProperty("socketTimeout", "3000"));
        connectionTimeout = connectionTimeout == 0 ? 1200 : connectionTimeout;
        socketTimeout = socketTimeout == 0 ? 3000 : socketTimeout;
        String url = "jdbc:tdsql-mysql://" + host + "/?useSSL=false&connectTimeout=" + connectionTimeout + "&socketTimeout=" + socketTimeout;
        int port = Integer.parseInt(prop.getProperty("PORT", "3306"));
        String db = prop.getProperty("DBNAME", "");
        return null ConnectionImpl.getInstance(host, port, prop, db, url);*/
    }

    public synchronized void addAllHost(List<String> l) {
        this.hosts.addAll(l);
    }

    public synchronized void addAllWeightFactor(List<String> l, List<String> wf) {
        if (wf == null || wf.isEmpty()) {
            for (String h : l) {
                this.weightFactor.put(h, 1);
            }
            return;
        }
        for (int i = 0; i < l.size(); i++) {
            if (i >= wf.size()) {
                this.weightFactor.put(l.get(i), 1);
            } else {
                this.weightFactor.put(l.get(i), Integer.parseInt(wf.get(i)));
            }
        }
    }

    public Set<String> getAllHost() {
        return this.hosts;
    }

    public Map<String, Integer> getWeightFactor() {
        return this.weightFactor;
    }

    public void log(Connection newConn, String message) {
        if (newConn instanceof com.tencentcloud.tdsql.mysql.cj.MysqlConnection) {
            ((MysqlConnection) newConn).getSession().getLog().logError(message);
        }
    }

    private class BlackListTask implements Runnable {

        private final HostInfo hostInfo;
        private final String bl;
        private final int blacklistTimeout;

        public BlackListTask(HostInfo hostInfo, String hl, int time) {
            this.hostInfo = hostInfo;
            this.bl = hl;
            this.blacklistTimeout = time;
        }

        @Override
        public void run() {
            if (ConnectionManager.getInstance().blackList.contains(this.bl)) {
                Connection conn = null;
                try {
                    conn = ConnectionManager.this.copyConn(this.hostInfo, this.bl);
                    if (conn != null) {
                        Set<String> set = ConnectionManager.getInstance().blackList;
                        synchronized (set) {
                            if (ConnectionManager.getInstance().blackList.contains(this.bl)) {
                                for (String key : ConnectionManager.HOST_CONNECTION_COUNT_MAP.keySet()) {
                                    ConnectionManager.HOST_CONNECTION_COUNT_MAP.put(key, 0);
                                }
                                ConnectionManager.getInstance().blackList.remove(this.bl);
                            }
                            return;
                        }
                    }
                    ConnectionManager.this.executor.schedule(
                            new BlackListTask(this.hostInfo, this.bl, this.blacklistTimeout), this.blacklistTimeout,
                            TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    ConnectionManager.this.executor.schedule(
                            new BlackListTask(this.hostInfo, this.bl, this.blacklistTimeout), this.blacklistTimeout,
                            TimeUnit.MILLISECONDS);
                    try {
                        ConnectionManager.this.log(conn,
                                "BlackListTask [" + ConnectionManager.getInstance().blackList + "]");
                        if (conn != null && !conn.isClosed()) {
                            conn.close();
                        }
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                } finally {
                    try {
                        ConnectionManager.this.log(conn,
                                "BlackListTask [" + ConnectionManager.getInstance().blackList + "]");
                        if (conn != null && !conn.isClosed()) {
                            conn.close();
                        }
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                }

            }
        }
    }

    public void setHaLoadBalanceMaximumErrorRetries(int haLoadBalanceMaximumErrorRetries) {
        this.haLoadBalanceMaximumErrorRetries = haLoadBalanceMaximumErrorRetries;
        if (this.haLoadBalanceMaximumErrorRetries <= 0) {
            this.haLoadBalanceMaximumErrorRetries = 1;
        }
    }

    public void setHaLoadBalanceHeartbeatIntervalTime(int haLoadBalanceHeartbeatIntervalTime) {
        this.haLoadBalanceHeartbeatIntervalTime = haLoadBalanceHeartbeatIntervalTime;
        if (this.haLoadBalanceHeartbeatIntervalTime <= 0) {
            this.haLoadBalanceHeartbeatIntervalTime = 3000;
        }
    }

    public void setHaLoadBalanceBlacklistTimeout(int haLoadBalanceBlacklistTimeout) {
        this.haLoadBalanceBlacklistTimeout = haLoadBalanceBlacklistTimeout;
        if (this.haLoadBalanceBlacklistTimeout <= 0) {
            this.haLoadBalanceBlacklistTimeout = 5000;
        }
    }

    public void setHaLoadBalanceHeartbeatMonitor(boolean haLoadBalanceHeartbeatMonitor) {
        this.haLoadBalanceHeartbeatMonitor = haLoadBalanceHeartbeatMonitor;
    }
}
