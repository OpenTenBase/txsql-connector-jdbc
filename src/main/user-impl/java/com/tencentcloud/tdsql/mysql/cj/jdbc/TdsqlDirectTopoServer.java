package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private TdsqlDirectTopoServer() {
    }

    public static TdsqlDirectTopoServer getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized void initialize(ConnectionUrl connectionUrl) {
        JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
        connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

        if (tdsqlProxyHostList == null) {
            tdsqlProxyHostList = connectionUrl.getHostsList();
        }

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
            initializeScheduler();
            topoServerSchedulerInitialized = true;
        }
    }

    private void initializeScheduler() {
        topoServerScheduler = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setNameFormat("TopoServer-pool-").build());
        topoServerScheduler.schedule(new TopoRefreshTask(tdsqlProxyTopoRefreshInterval),
                0L, TimeUnit.MILLISECONDS);
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

    // -----------------------------------------------------------------------------------------------------------------
    // TODO: other methods
    // ......
    // -----------------------------------------------------------------------------------------------------------------

    private static class TopoRefreshTask implements Runnable {

        private final Integer tdsqlProxyTopoRefreshInterval;

        public TopoRefreshTask(Integer tdsqlProxyTopoRefreshInterval) {
            this.tdsqlProxyTopoRefreshInterval = tdsqlProxyTopoRefreshInterval;
        }

        @Override
        public void run() {
            // TODO: Refresh topology structure logic.
            System.out.println("tdsqlProxyTopoRefreshInterval = " + tdsqlProxyTopoRefreshInterval);
        }
    }

    private static class SingletonInstance {

        private static final TdsqlDirectTopoServer INSTANCE = new TdsqlDirectTopoServer();
    }
}
