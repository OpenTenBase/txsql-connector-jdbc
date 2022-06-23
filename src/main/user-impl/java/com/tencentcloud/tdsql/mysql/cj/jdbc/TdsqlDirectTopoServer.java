package com.tencentcloud.tdsql.mysql.cj.jdbc;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_COLUMN_CLUSTER_NAME;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_COLUMN_MASTER_IP;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_COLUMN_SLAVE_IP_LIST;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_CONNECTION_TIMEOUT_MILLISECONDS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_SQL;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_SHOW_ROUTES_STATEMENT_TIMEOUT_SECONDS;

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
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst;
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
    private String tdsqlReadWriteMode = TdsqlConst.TDSQL_READ_WRITE_MODE_RW;
    private Integer tdsqlMaxSlaveDelay = TdsqlConst.TDSQL_MAX_SLAVE_DELAY_DEFAULT_VALUE;
    private Connection proxyConnection;
    private Integer tdsqlProxyTopoRefreshInterval = TdsqlConst.TDSQL_PROXY_TOPO_REFRESH_INTERVAL_DEFAULT_VALUE;
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

            String newTdsqlReadWriteMode = connProps.getStringProperty(PropertyKey.tdsqlReadWriteMode).getValue();
            if (!tdsqlReadWriteMode.equalsIgnoreCase(newTdsqlReadWriteMode)) {
                if (TdsqlConst.TDSQL_READ_WRITE_MODE_RW.equalsIgnoreCase(newTdsqlReadWriteMode)
                        || TdsqlConst.TDSQL_READ_WRITE_MODE_RO.equalsIgnoreCase(newTdsqlReadWriteMode)) {
                    tdsqlReadWriteMode = newTdsqlReadWriteMode;
                }
            }

            if (TdsqlConst.TDSQL_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlReadWriteMode)) {
                Integer newTdsqlMaxSlaveDelay = connProps.getIntegerProperty(PropertyKey.tdsqlMaxSlaveDelay).getValue();
                if (!tdsqlMaxSlaveDelay.equals(newTdsqlMaxSlaveDelay)) {
                    if (newTdsqlMaxSlaveDelay > 0 && newTdsqlMaxSlaveDelay < Integer.MAX_VALUE) {
                        tdsqlMaxSlaveDelay = newTdsqlMaxSlaveDelay;
                    }
                }
            }

            Integer newTdsqlProxyTopoRefreshInterval = connProps
                    .getIntegerProperty(PropertyKey.tdsqlProxyTopoRefreshInterval).getValue();
            if (!tdsqlProxyTopoRefreshInterval.equals(newTdsqlProxyTopoRefreshInterval)) {
                if (newTdsqlProxyTopoRefreshInterval > 0 && newTdsqlProxyTopoRefreshInterval < Integer.MAX_VALUE) {
                    tdsqlProxyTopoRefreshInterval = newTdsqlProxyTopoRefreshInterval;
                    if (topoServerInitialized.compareAndSet(true, false) && topoServerScheduler != null) {
                        topoServerScheduler.shutdown();
                    }
                }
            }

            if (topoServerInitialized.compareAndSet(false, true)) {
                initProxyConnection();
                initializeScheduler();
                DataSetCache.getInstance().addListener(
                        new UpdateSchedulingQueueCacheListener(tdsqlReadWriteMode, scheduleQueue, connectionUrl));
                DataSetCache.getInstance().addListener(new FailoverCacheListener(tdsqlReadWriteMode));
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

    private void initProxyConnection() throws SQLException {
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
        config.put(PropertyKey.loadBalanceBlocklistTimeout.getKeyName(), "5000");
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
            TdsqlDirectLoggerFactory.logDebug("Proxy connection is invalid, recreate it!");
            try {
                proxyConnection.close();
            } catch (SQLException e) {
                // ignore
            } finally {
                initProxyConnection();
            }
        }

        List<DataSetCluster> dataSetClusters = new ArrayList<>();
        proxyConnection.setNetworkTimeout(netTimeoutExecutor, TDSQL_SHOW_ROUTES_CONNECTION_TIMEOUT_MILLISECONDS);
        try (Statement stmt = proxyConnection.createStatement()) {
            stmt.setQueryTimeout(TDSQL_SHOW_ROUTES_STATEMENT_TIMEOUT_SECONDS);
            try (ResultSet rs = stmt.executeQuery(TDSQL_SHOW_ROUTES_SQL)) {
                while (rs.next()) {
                    String clusterName = rs.getString(TDSQL_SHOW_ROUTES_COLUMN_CLUSTER_NAME);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: cluster name is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
                    String master = rs.getString(TDSQL_SHOW_ROUTES_COLUMN_MASTER_IP);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: master ip is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
                    String slaves = rs.getString(TDSQL_SHOW_ROUTES_COLUMN_SLAVE_IP_LIST);
                    if (StringUtils.isNullOrEmpty(clusterName)) {
                        String errMsg = "Get topology error: slave ip list is null!";
                        TdsqlDirectLoggerFactory.logError(errMsg);
                        throw new TdsqlSyncBackendTopoException(errMsg);
                    }
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
        topoServerScheduler.scheduleWithFixedDelay(new TopoRefreshTask(), 0L, tdsqlProxyTopoRefreshInterval,
                TimeUnit.MILLISECONDS);
    }

    public String getTdsqlReadWriteMode() {
        return tdsqlReadWriteMode;
    }

    public Integer getTdsqlMaxSlaveDelay() {
        return tdsqlMaxSlaveDelay;
    }

    public Integer getTdsqlProxyTopoRefreshInterval() {
        return tdsqlProxyTopoRefreshInterval;
    }

    public ReentrantReadWriteLock getRefreshLock() {
        return refreshLock;
    }

    public TdsqlAtomicLongMap<TdsqlHostInfo> getScheduleQueue() {
        return scheduleQueue;
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
