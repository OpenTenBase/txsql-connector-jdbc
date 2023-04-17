package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectDataSourceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandlerImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;

/**
 * <p>TDSQL专属，直连模式数据源类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectDataSource {

    public String getDataSourceUuid() {
        return dataSourceUuid;
    }

    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final AtomicBoolean isInitialized;
    private final AtomicBoolean isActived;
    private CountDownLatch countDownLatch;
    private Throwable lastException;

    private final ReentrantReadWriteLock lock;

    public TdsqlDirectDataSource(String dataSourceUuid) {
        this.dataSourceUuid = dataSourceUuid;
        this.dataSourceConfig = new TdsqlDirectDataSourceConfig(dataSourceUuid);
        this.isInitialized = new AtomicBoolean(false);
        this.isActived = new AtomicBoolean(true);
        this.countDownLatch = new CountDownLatch(1);
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * 初始化数据源
     *
     * @param connectionUrl {@link ConnectionUrl}
     */
    public void initialize(ConnectionUrl connectionUrl) {
        if (this.isInitialized.compareAndSet(false, true)) {
            try {
                // URL参数校验并赋值
                this.dataSourceConfig.validateConnectionProperties(connectionUrl);

                // 初始化拓扑刷新服务并赋值
                TdsqlDirectTopologyServer topologyServer = new TdsqlDirectTopologyServer(this.dataSourceConfig);
                this.dataSourceConfig.setTopologyServer(topologyServer);

                // 初始化调度服务并赋值
                TdsqlDirectScheduleServer scheduleServer = new TdsqlDirectScheduleServer(this.dataSourceConfig);
                this.dataSourceConfig.setScheduleServer(scheduleServer);

                TdsqlDirectFailoverHandler failoverHandler = new TdsqlDirectFailoverHandlerImpl(this.dataSourceConfig);
                this.dataSourceConfig.setFailoverHandler(failoverHandler);

                // 初始化拓扑缓存服务并赋值
                TdsqlDirectCacheServer cacheServer = new TdsqlDirectCacheServer(this.dataSourceConfig);
                this.dataSourceConfig.setCacheServer(cacheServer);
                this.countDownLatch.countDown();

                // 初始化连接管理器并赋值
                TdsqlDirectConnectionManager connectionManager = new TdsqlDirectConnectionManager(this.dataSourceConfig);
                this.dataSourceConfig.setConnectionManager(connectionManager);

                // 开始刷新拓扑信息
                topologyServer.startRefreshTopology();
            } catch (Throwable t) {
                if (t.getCause() != null) {
                    lastException = t.getCause();
                } else {
                    lastException = t;
                }
                throw new RuntimeException(lastException);
            }

        } else {
            // 数据源在初始化之后，不允许再次调用初始化方法
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectDataSourceException.class,
                    Messages.getString("TdsqlDirectDataSourceException.RepeatedInitialization",
                            new Object[]{this.dataSourceUuid}));
        }
    }

    public boolean waitForFirstFinished() {
        try {
            if (!this.countDownLatch.await(1000, TimeUnit.MILLISECONDS)) {
                if (this.lastException != null) {
                    logError(this.lastException);
                    throw new RuntimeException(this.lastException);
                }
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                        "init tdsql direct datasource failed! wait timeout: 1000ms");
            }
        } catch  (InterruptedException e) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                    "init tdsql direct datasource failed! interrupted");
        }

        return this.getCacheServer().waitForFirstFinished();
    }


    public TdsqlDirectCacheServer getCacheServer() {
        return this.dataSourceConfig.getCacheServer();
    }

    public TdsqlDirectConnectionManager getConnectionManager() {
        return this.dataSourceConfig.getConnectionManager();
    }

    public TdsqlDirectScheduleServer getScheduleServer() {
        return this.dataSourceConfig.getScheduleServer();
    }

    public ReadWriteLock getLock() {
        return this.lock;
    }

    public ReentrantReadWriteLock.ReadLock getReadLock() {
        return this.lock.readLock();
    }

    public ReentrantReadWriteLock.WriteLock getWriteLock() {
        return this.lock.writeLock();
    }

    public void setActiveState(boolean state) {
        this.isActived.set(state);
    }

    public boolean getActiveState() {
        return this.isActived.get();
    }

    public boolean shouldBeClosed() {
        return (this.getConnectionManager().getLiveConnectionMap().size() == 0 &
                this.getConnectionManager().getLastEmptyLiveConnectionTimestamp() != 0 &
                System.currentTimeMillis() - this.getConnectionManager().getLastEmptyLiveConnectionTimestamp() > 1000 * 60);
    }

    public void close() {
        this.dataSourceConfig.getTopologyServer().stopRefreshTopology();
        this.dataSourceConfig.getTopologyServer().closeAllProxyConnections();
    }
}
