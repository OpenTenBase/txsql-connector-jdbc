package com.tencentcloud.tdsql.mysql.cj.jdbc;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_CLOSE_CONN_TIMEOUT_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RW;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_SHOW_ROUTES_SQL;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_COLUMN_CLUSTER_NAME;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_COLUMN_MASTER_IP;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_COLUMN_SLAVE_IP_LIST;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_REFRESH_CONN_TIMEOUT_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_REFRESH_INTERVAL_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst.TDSQL_DIRECT_TOPO_REFRESH_STMT_TIMEOUT_SECONDS;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.url.LoadBalanceConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCluster;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TdsqlSyncBackendTopoException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.FailoverCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.UpdateSchedulingQueueCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.AbstractTdsqlCaughtRunnable;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.SynchronousExecutor;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectTopoServer {

    private ScheduledThreadPoolExecutor topoServerScheduler = null;
    private String tdsqlDirectReadWriteMode = TDSQL_DIRECT_READ_WRITE_MODE_RW;
    private Integer tdsqlDirectMaxSlaveDelaySeconds = TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS;
    private Integer tdsqlDirectTopoRefreshIntervalMillis = TDSQL_DIRECT_TOPO_REFRESH_INTERVAL_MILLIS;
    private Integer tdsqlDirectTopoRefreshConnTimeoutMillis = TDSQL_DIRECT_TOPO_REFRESH_CONN_TIMEOUT_MILLIS;
    private Integer tdsqlDirectTopoRefreshStmtTimeoutSeconds = TDSQL_DIRECT_TOPO_REFRESH_STMT_TIMEOUT_SECONDS;
    private Integer tdsqlDirectCloseConnTimeoutMillis = TDSQL_DIRECT_CLOSE_CONN_TIMEOUT_MILLIS;
    private Connection proxyConnection;
    private ConnectionUrl connectionUrl = null;
    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlAtomicLongMap.create();
    private final ReentrantReadWriteLock refreshLock = new ReentrantReadWriteLock();
    private final AtomicBoolean topoServerInitialized = new AtomicBoolean(false);
    private final Executor netTimeoutExecutor = new SynchronousExecutor();

    private TdsqlDirectTopoServer() {
    }

    public static TdsqlDirectTopoServer getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public void initialize(ConnectionUrl connectionUrl) throws SQLException {
        refreshLock.writeLock().lock();
        try {
            this.connectionUrl = connectionUrl;
            JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
            connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

            String newTdsqlReadWriteMode = connProps.getStringProperty(PropertyKey.tdsqlDirectReadWriteMode).getValue();
            if (!tdsqlDirectReadWriteMode.equalsIgnoreCase(newTdsqlReadWriteMode)) {
                if (TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(newTdsqlReadWriteMode)
                        || TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(newTdsqlReadWriteMode)) {
                    tdsqlDirectReadWriteMode = newTdsqlReadWriteMode;
                }
            }

            if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode)) {
                Integer newTdsqlMaxSlaveDelay = connProps.getIntegerProperty(
                        PropertyKey.tdsqlDirectMaxSlaveDelaySeconds).getValue();
                if (!tdsqlDirectMaxSlaveDelaySeconds.equals(newTdsqlMaxSlaveDelay)) {
                    if (newTdsqlMaxSlaveDelay > 0 && newTdsqlMaxSlaveDelay < Integer.MAX_VALUE) {
                        tdsqlDirectMaxSlaveDelaySeconds = newTdsqlMaxSlaveDelay;
                    }
                }
            }

            Integer newTdsqlProxyTopoRefreshInterval = connProps.getIntegerProperty(
                    PropertyKey.tdsqlDirectTopoRefreshIntervalMillis).getValue();
            if (!tdsqlDirectTopoRefreshIntervalMillis.equals(newTdsqlProxyTopoRefreshInterval)) {
                if (newTdsqlProxyTopoRefreshInterval > 0 && newTdsqlProxyTopoRefreshInterval < Integer.MAX_VALUE) {
                    tdsqlDirectTopoRefreshIntervalMillis = newTdsqlProxyTopoRefreshInterval;
                    if (topoServerInitialized.compareAndSet(true, false) && topoServerScheduler != null) {
                        topoServerScheduler.shutdown();
                    }
                }
            }

            Integer newTdsqlDirectTopoRefreshConnTimeoutMillis = connProps.getIntegerProperty(
                    PropertyKey.tdsqlDirectTopoRefreshConnTimeoutMillis).getValue();
            if (!tdsqlDirectTopoRefreshConnTimeoutMillis.equals(newTdsqlDirectTopoRefreshConnTimeoutMillis)) {
                if (newTdsqlDirectTopoRefreshConnTimeoutMillis > 250
                        && newTdsqlDirectTopoRefreshConnTimeoutMillis < Integer.MAX_VALUE) {
                    tdsqlDirectTopoRefreshConnTimeoutMillis = newTdsqlDirectTopoRefreshConnTimeoutMillis;
                }
            }

            Integer newTdsqlDirectTopoRefreshStmtTimeoutSeconds = connProps.getIntegerProperty(
                    PropertyKey.tdsqlDirectTopoRefreshStmtTimeoutSeconds).getValue();
            if (!tdsqlDirectTopoRefreshStmtTimeoutSeconds.equals(newTdsqlDirectTopoRefreshStmtTimeoutSeconds)) {
                if (newTdsqlDirectTopoRefreshStmtTimeoutSeconds > 0
                        && newTdsqlDirectTopoRefreshStmtTimeoutSeconds < Integer.MAX_VALUE) {
                    tdsqlDirectTopoRefreshStmtTimeoutSeconds = newTdsqlDirectTopoRefreshStmtTimeoutSeconds;
                }
            }

            Integer newTdsqlDirectCloseConnTimeoutMillis = connProps.getIntegerProperty(
                    PropertyKey.tdsqlDirectCloseConnTimeoutMillis).getValue();
            if (!tdsqlDirectCloseConnTimeoutMillis.equals(newTdsqlDirectCloseConnTimeoutMillis)) {
                if (newTdsqlDirectCloseConnTimeoutMillis > 250
                        && newTdsqlDirectCloseConnTimeoutMillis < Integer.MAX_VALUE) {
                    tdsqlDirectCloseConnTimeoutMillis = newTdsqlDirectCloseConnTimeoutMillis;
                }
            }

            if (topoServerInitialized.compareAndSet(false, true)) {
                createProxyConnection();
                initializeScheduler();
                DataSetCache.getInstance().addListener(
                        new UpdateSchedulingQueueCacheListener(tdsqlDirectReadWriteMode, scheduleQueue, connectionUrl));
                DataSetCache.getInstance().addListener(new FailoverCacheListener(tdsqlDirectReadWriteMode));
            }
        } finally {
            refreshLock.writeLock().unlock();
        }
        if (!DataSetCache.getInstance().waitCached(1, 60)) {
            String errMsg = "wait tdsql topology timeout";
            TdsqlDirectLoggerFactory.logError(errMsg);
            throw new TdsqlSyncBackendTopoException(errMsg);
        }
    }

    private void createProxyConnection() throws SQLException {
        TdsqlDirectLoggerFactory.logDebug("Start create proxy connection for refresh topology!");
        if (proxyConnection != null && !proxyConnection.isClosed() && proxyConnection.isValid(1)) {
            TdsqlDirectLoggerFactory.logDebug("Proxy connection seems perfect, NOOP!");
            return;
        }

        String errMsg = "Create proxy connection for refresh topology error!";
        Map<String, String> config = new HashMap<>(8);
        config.put(PropertyKey.connectTimeout.getKeyName(), "2000");
        config.put(PropertyKey.socketTimeout.getKeyName(), "2000");
        config.put(PropertyKey.maxAllowedPacket.getKeyName(), "65535000");
        config.put(PropertyKey.retriesAllDown.getKeyName(), "4");
        config.put(PropertyKey.loadBalanceBlocklistTimeout.getKeyName(), "30000");
        config.put(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "1");
        config.put(PropertyKey.loadBalancePingTimeout.getKeyName(), "1000");
        config.put(PropertyKey.loadBalanceValidateConnectionOnSwapServer.getKeyName(), "true");
        LoadBalanceConnectionUrl myConnUrl = new LoadBalanceConnectionUrl(connectionUrl.getHostsList(), config);
        try {
            proxyConnection = LoadBalancedConnectionProxy.createProxyInstance(myConnUrl);
            if (!proxyConnection.isClosed() && proxyConnection.isValid(1)) {
                TdsqlDirectLoggerFactory.setLogger(((JdbcConnection) proxyConnection).getSession().getLog());
            } else {
                TdsqlDirectLoggerFactory.logError(errMsg);
                throw SQLError.createSQLException(Messages.getString("Connection.UnableToConnect"),
                        MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, null);
            }
        } catch (SQLException e) {
            TdsqlDirectLoggerFactory.logError("[" + errMsg + "]" + e.getMessage(), e);
            throw e;
        }
        TdsqlDirectLoggerFactory.logDebug("Finish create proxy connection for refresh topology!");
    }

    private void getTopology() throws SQLException {
        if (proxyConnection == null || proxyConnection.isClosed() || !proxyConnection.isValid(1)) {
            TdsqlDirectLoggerFactory.logDebug("Proxy connection is invalid, reconnection it!");
            try {
                proxyConnection.close();
            } catch (SQLException e) {
                // ignore
            } finally {
                createProxyConnection();
            }
        }

        List<DataSetCluster> dataSetClusters = new ArrayList<>();
        proxyConnection.setNetworkTimeout(netTimeoutExecutor, tdsqlDirectTopoRefreshConnTimeoutMillis);
        try (Statement stmt = proxyConnection.createStatement()) {
            stmt.setQueryTimeout(tdsqlDirectTopoRefreshStmtTimeoutSeconds);
            try (ResultSet rs = stmt.executeQuery(TDSQL_DIRECT_SHOW_ROUTES_SQL)) {
                while (rs.next()) {
                    String clusterName = rs.getString(TDSQL_DIRECT_TOPO_COLUMN_CLUSTER_NAME);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: cluster name is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
                    String master = rs.getString(TDSQL_DIRECT_TOPO_COLUMN_MASTER_IP);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: master ip is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
                    String slaves = rs.getString(TDSQL_DIRECT_TOPO_COLUMN_SLAVE_IP_LIST);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: slave ip list is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
                    TdsqlDirectLoggerFactory.logInfo(
                            "Topo info cluster name: " + clusterName + ", master: " + master + ", slaves: " + slaves);
                    DataSetCluster dataSetCluster = new DataSetCluster(clusterName);
                    dataSetCluster.setMaster(DataSetUtil.parseMaster(master));
                    dataSetCluster.setSlaves(DataSetUtil.parseSlaveList(slaves));
                    dataSetClusters.add(dataSetCluster);
                }
            }
        }
        if (dataSetClusters.isEmpty()) {
            String errMsg = "No backend cluster found with command: /*proxy*/ show routes";
            TdsqlDirectLoggerFactory.logError(errMsg);
            throw new TdsqlSyncBackendTopoException(errMsg);
        }

        DataSetCache cache = DataSetCache.getInstance();
        if (dataSetClusters.get(0).getMaster() != null) {
            cache.setMasters(Collections.singletonList(dataSetClusters.get(0).getMaster()));
        } else {
            cache.setMasters(new ArrayList<>());
        }
        cache.setSlaves(dataSetClusters.get(0).getSlaves());
    }

    private void initializeScheduler() {
        topoServerScheduler = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("TopoServer-pool-%d").build());
        topoServerScheduler.scheduleWithFixedDelay(new TopoRefreshTask(), 0L, tdsqlDirectTopoRefreshIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    public String getTdsqlDirectReadWriteMode() {
        return tdsqlDirectReadWriteMode;
    }

    public Integer getTdsqlDirectMaxSlaveDelaySeconds() {
        return tdsqlDirectMaxSlaveDelaySeconds;
    }

    public Integer getTdsqlDirectTopoRefreshIntervalMillis() {
        return tdsqlDirectTopoRefreshIntervalMillis;
    }

    public ReentrantReadWriteLock getRefreshLock() {
        return refreshLock;
    }

    public TdsqlAtomicLongMap<TdsqlHostInfo> getScheduleQueue() {
        return scheduleQueue;
    }

    public Integer getTdsqlDirectCloseConnTimeoutMillis() {
        return tdsqlDirectCloseConnTimeoutMillis;
    }

    private static class TopoRefreshTask extends AbstractTdsqlCaughtRunnable {

        @Override
        public void caughtAndRun() {
            String proxyHost = ((JdbcConnection) getInstance().proxyConnection).getHostPortPair();
            TdsqlDirectLoggerFactory.logDebug("Start topology refresh task. Request proxy: [" + proxyHost + "]");
            try {
                TdsqlDirectTopoServer.getInstance().getTopology();
            } catch (Exception e) {
                TdsqlDirectLoggerFactory.logError(e.getMessage(), e);
            }
            TdsqlDirectLoggerFactory.logDebug("Finish topology refresh task. Request proxy: [" + proxyHost + "]");
        }
    }

    private static class SingletonInstance {

        private static final TdsqlDirectTopoServer INSTANCE = new TdsqlDirectTopoServer();
    }
}
