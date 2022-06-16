package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.url.LoadBalanceConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCluster;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TDSQLSyncBackendTopoException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.FailoverCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.UpdateSchedulingQueueCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private String tdsqlReadWriteMode = TdsqlDirectConst.TDSQL_READ_WRITE_MODE_RW;
    private Integer tdsqlMaxSlaveDelay = TdsqlDirectConst.TDSQL_MAX_SLAVE_DELAY_DEFAULT_VALUE;
    private Connection tdsqlConnection;
    private Integer tdsqlProxyTopoRefreshInterval = TdsqlDirectConst.TDSQL_PROXY_TOPO_REFRESH_INTERVAL_DEFAULT_VALUE;
    private ConnectionUrl connectionUrl = null;
    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlAtomicLongMap.create();
    private final ReentrantReadWriteLock refreshLock = new ReentrantReadWriteLock();
    private final AtomicBoolean topoServerInitialized = new AtomicBoolean(false);

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
                if (TdsqlDirectConst.TDSQL_READ_WRITE_MODE_RW.equalsIgnoreCase(newTdsqlReadWriteMode)
                        || TdsqlDirectConst.TDSQL_READ_WRITE_MODE_RO.equalsIgnoreCase(newTdsqlReadWriteMode)) {
                    tdsqlReadWriteMode = newTdsqlReadWriteMode;
                }
            }

            if (TdsqlDirectConst.TDSQL_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlReadWriteMode)) {
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
                initTdsqlConnection();
                initializeScheduler();
                DataSetCache.getInstance().addListener(
                        new UpdateSchedulingQueueCacheListener(tdsqlReadWriteMode, scheduleQueue, connectionUrl));
                DataSetCache.getInstance().addListener(new FailoverCacheListener(tdsqlReadWriteMode));
            }
        } finally {
            refreshLock.writeLock().unlock();
        }
        if (!DataSetCache.getInstance().waitCached(1, 60)) {
            TdsqlDirectLoggerFactory.logError("wait tdsql topology timeout");
            throw new SQLException("wait tdsql topology timeout");
        }
    }

    private void initTdsqlConnection() throws SQLException {
        if (tdsqlConnection != null && !tdsqlConnection.isClosed() && tdsqlConnection.isValid(1)) {
            return;
        }
        Map<String, String> config = new HashMap<>(3);
        config.put(PropertyKey.retriesAllDown.getKeyName(), "1");
        config.put(PropertyKey.connectTimeout.getKeyName(), "1000");
        config.put(PropertyKey.maxAllowedPacket.getKeyName(), "65535000");
        LoadBalanceConnectionUrl myConnUrl = new LoadBalanceConnectionUrl(connectionUrl.getHostsList(), config);
        try {
            tdsqlConnection = LoadBalancedConnectionProxy.createProxyInstance(myConnUrl);
            if (!tdsqlConnection.isClosed()) {
                TdsqlDirectLoggerFactory.setLogger(((JdbcConnection) tdsqlConnection).getSession().getLog());
            }
        } catch (SQLException e) {
            TdsqlDirectConnectionManager.getInstance().closeAll();
            throw e;
        }
    }

    private void getTopology() throws SQLException {
        if (tdsqlConnection == null || tdsqlConnection.isClosed() || !tdsqlConnection.isValid(1)) {
            initTdsqlConnection();
        }
        List<DataSetCluster> dataSetClusters = TdsqlUtil.showRoutes(tdsqlConnection);
        if (dataSetClusters.isEmpty()) {
            TdsqlDirectLoggerFactory.logError("No backend cluster found with command: /*proxy*/ show routes");
            throw new TDSQLSyncBackendTopoException("No backend cluster found with command: /*proxy*/ show routes");
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

    private static class TopoRefreshTask implements Runnable {

        @Override
        public void run() {
            try {
                TdsqlDirectTopoServer.getInstance().getTopology();
            } catch (Exception e) {
                TdsqlDirectLoggerFactory.logError(e.getMessage(), e);
            }
        }
    }

    private static class SingletonInstance {

        private static final TdsqlDirectTopoServer INSTANCE = new TdsqlDirectTopoServer();
    }
}
