package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
    private Integer tdsqlProxyTopoRefreshInterval = TdsqlConst.TDSQL_PROXY_TOPO_REFRESH_INTERVAL_DEFAULT_VALUE;
    private final ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue = new ConcurrentHashMap<>();
    public ReentrantLock lock = new ReentrantLock();

    private TdsqlDirectTopoServer() {
    }

    public static TdsqlDirectTopoServer getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized void initialize(ConnectionUrl connectionUrl) {
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
            getTopology(connectionUrl, true);
            initializeScheduler(connectionUrl);
            topoServerSchedulerInitialized = true;
        }
    }

    private void getTopology(ConnectionUrl connectionUrl, Boolean firstInitialize) {
        if (firstInitialize) {
            scheduleQueue.clear();
            refreshTopology(connectionUrl);
        } else {
            // TODO: 模拟建连、获取返回值、比较等操作，如发现拓扑变化，再调用 refreshTopology() 方法
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void refreshTopology(ConnectionUrl connectionUrl) {
        TdsqlDirectTopoServer.getInstance().lock.lock();
        try {
            HostInfo hostInfo = connectionUrl.getMainHost();
            DatabaseUrlContainer originalUrl = hostInfo.getOriginalUrl();
            String username = hostInfo.getUser();
            String password = hostInfo.getPassword();
            Map<String, String> properties = hostInfo.getHostProperties();

            scheduleQueue.put(new TdsqlHostInfo(
                    new HostInfo(originalUrl, "9.134.209.89", 3357, username, password, properties)), 0L);
            scheduleQueue.put(new TdsqlHostInfo(
                    new HostInfo(originalUrl, "9.134.209.89", 3358, username, password, properties)), 0L);
            scheduleQueue.put(new TdsqlHostInfo(
                    new HostInfo(originalUrl, "9.134.209.89", 3359, username, password, properties)), 0L);
            scheduleQueue.put(new TdsqlHostInfo(
                    new HostInfo(originalUrl, "9.134.209.89", 3360, username, password, properties)), 0L);
        } finally {
            TdsqlDirectTopoServer.getInstance().lock.unlock();
        }
    }

    private void initializeScheduler(ConnectionUrl connectionUrl) {
        topoServerScheduler = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setNameFormat("TopoServer-pool-").build());
        topoServerScheduler.scheduleAtFixedRate(new TopoRefreshTask(connectionUrl, scheduleQueue), 0L,
                tdsqlProxyTopoRefreshInterval, TimeUnit.MILLISECONDS);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // TODO: other methods
    // ......
    // -----------------------------------------------------------------------------------------------------------------

    public String getTdsqlReadWriteMode() {
        return tdsqlReadWriteMode;
    }

    public Integer getTdsqlMaxSlaveDelay() {
        return tdsqlMaxSlaveDelay;
    }

    public Integer getTdsqlProxyTopoRefreshInterval() {
        return tdsqlProxyTopoRefreshInterval;
    }

    public ConcurrentHashMap<TdsqlHostInfo, Long> getScheduleQueue() {
        return scheduleQueue;
    }

    private static class TopoRefreshTask implements Runnable {

        private final ConnectionUrl connectionUrl;
        private final ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue;

        public TopoRefreshTask(ConnectionUrl connectionUrl, ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue) {
            this.connectionUrl = connectionUrl;
            this.scheduleQueue = scheduleQueue;
        }

        @Override
        public void run() {
            // TODO: Refresh topology structure logic.
            try {
                TdsqlDirectTopoServer.getInstance().getTopology(connectionUrl, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class SingletonInstance {

        private static final TdsqlDirectTopoServer INSTANCE = new TdsqlDirectTopoServer();
    }
}
