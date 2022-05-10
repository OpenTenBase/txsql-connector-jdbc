package com.tencentcloud.tdsql.mysql.cj.jdbc;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectMasterSlaveSwitchMode.SLAVE_OFFLINE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectMasterSlaveSwitchMode.SLAVE_ONLINE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RO;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCluster;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TDSQLSyncBackendTopoException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlDirectFailoverOperator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.FailoverCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.listener.UpdateSchedulingQueueCacheListener;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectTopoServer {

    private volatile boolean topoServerSchedulerInitialized = false;
    private ScheduledThreadPoolExecutor topoServerScheduler = null;
    private List<HostInfo> tdsqlProxyHostList = null;
    private String tdsqlReadWriteMode = TdsqlConst.TDSQL_READ_WRITE_MODE_RW;
    private Integer tdsqlMaxSlaveDelay = TdsqlConst.TDSQL_MAX_SLAVE_DELAY_DEFAULT_VALUE;
    public ReentrantLock lock = new ReentrantLock();
    private Connection tdsqlConnection;
    private Integer tdsqlProxyTopoRefreshInterval = TdsqlConst.TDSQL_PROXY_TOPO_REFRESH_INTERVAL_DEFAULT_VALUE * 100000;
    private ConnectionUrl connectionUrl = null;
    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlAtomicLongMap.create();
    private final ReentrantReadWriteLock refreshLock = new ReentrantReadWriteLock();

    private TdsqlDirectTopoServer() {
    }

    public static TdsqlDirectTopoServer getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized void initialize(ConnectionUrl connectionUrl) throws SQLException {
        this.connectionUrl = connectionUrl;
        JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
        connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

        tdsqlProxyHostList = connectionUrl.getHostsList();

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
                if (topoServerSchedulerInitialized && topoServerScheduler != null) {
                    topoServerScheduler.shutdown();
                    topoServerSchedulerInitialized = false;
                }
            }
        }

        if (!topoServerSchedulerInitialized) {
            topoServerSchedulerInitialized = true;
            tdsqlConnection = LoadBalancedConnectionProxy.createProxyInstance(connectionUrl);
            initializeScheduler();
            DataSetCache.getInstance().addListener(
                    new UpdateSchedulingQueueCacheListener(tdsqlReadWriteMode, scheduleQueue, connectionUrl));
            DataSetCache.getInstance().addListener(new FailoverCacheListener(tdsqlReadWriteMode));
        }
        if (!DataSetCache.getInstance().waitCached(1, 60)) {
            throw new SQLException("wait tdsql topology timeout");
        }
    }

    private void getTopology() throws SQLException {
        if(tdsqlConnection == null) {
            return;
        }
        List<DataSetCluster> dataSetClusters = TdsqlUtil.showRoutes(tdsqlConnection);
        if (dataSetClusters.isEmpty()) {
            throw new TDSQLSyncBackendTopoException("No backend cluster found with command: /*proxy*/ show routes");
        }
        if (dataSetClusters.get(0).getMaster() != null) {
            DataSetCache.getInstance().setMasters(Collections.singletonList(dataSetClusters.get(0).getMaster()));
        } else {
            DataSetCache.getInstance().setMasters(new ArrayList<>());
        }

        DataSetCache.getInstance().setSlaves(dataSetClusters.get(0).getSlaves());
    }

    int cnt = 0;

    public void refreshTopology(Boolean firstInitialize) {
        refreshLock.writeLock().lock();
        try {
            HostInfo mainHost = connectionUrl.getMainHost();
            DatabaseUrlContainer originalUrl = mainHost.getOriginalUrl();
            String username = mainHost.getUser();
            String password = mainHost.getPassword();
            Map<String, String> properties = mainHost.getHostProperties();

            if (firstInitialize) {
                scheduleQueue.clear();
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3357, username, password, properties)), 0L);
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3358, username, password, properties)), 0L);
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3359, username, password, properties)), 0L);
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3360, username, password, properties)), 0L);
            } else if (cnt % 2 > 0) {
                for (Entry<TdsqlHostInfo, Long> entry : scheduleQueue.asMap().entrySet()) {
                    if ("9.134.209.89:3357".equalsIgnoreCase(entry.getKey().getHostPortPair())
                            || "9.134.209.89:3358".equalsIgnoreCase(entry.getKey().getHostPortPair())) {
                        scheduleQueue.remove(entry.getKey());
                    }
                }
                TdsqlDirectFailoverOperator.subsequentOperation(RO, SLAVE_OFFLINE, new ArrayList<String>() {{
                    add("9.134.209.89:3357");
                    add("9.134.209.89:3358");
                }});
            } else if (cnt % 2 == 0) {
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3357, username, password, properties)), 0L);
                scheduleQueue.put(new TdsqlHostInfo(
                        new HostInfo(originalUrl, "9.134.209.89", 3358, username, password, properties)), 0L);
                TdsqlDirectFailoverOperator.subsequentOperation(RO, SLAVE_ONLINE, new ArrayList<String>() {{
                    add("9.134.209.89:3357");
                    add("9.134.209.89:3358");
                }});
            }
            ++cnt;
            System.out.println("=======================================================================> cnt = " + cnt);
        } finally {
            for (Entry<TdsqlHostInfo, Long> entry : scheduleQueue.asMap().entrySet()) {
                System.out.println(
                        "ScheduleQueue,Host:Count = " + entry.getKey().getHostPortPair() + ":" + entry.getValue());
            }
            refreshLock.writeLock().unlock();
        }
    }

    private void initializeScheduler() {
        topoServerScheduler = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setNameFormat("TopoServer-pool-").build());
        topoServerScheduler.scheduleAtFixedRate(new TopoRefreshTask(), 0L, tdsqlProxyTopoRefreshInterval,
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
                e.printStackTrace();
            }
        }
    }

    private static class SingletonInstance {

        private static final TdsqlDirectTopoServer INSTANCE = new TdsqlDirectTopoServer();
    }
}
