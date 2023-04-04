package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectDataSourceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandlerImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverMasterHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverSlavesHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>TDSQL专属，直连模式数据源类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectDataSource {

    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final AtomicBoolean isInitialized;

    private CountDownLatch countDownLatch;

    public TdsqlDirectDataSource(String dataSourceUuid) {
        this.dataSourceUuid = dataSourceUuid;
        this.dataSourceConfig = new TdsqlDirectDataSourceConfig(dataSourceUuid);
        this.isInitialized = new AtomicBoolean(false);
        this.countDownLatch = new CountDownLatch(1);
    }

    /**
     * 初始化数据源
     *
     * @param connectionUrl {@link ConnectionUrl}
     */
    public void initialize(ConnectionUrl connectionUrl) {
        if (this.isInitialized.compareAndSet(false, true)) {

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
}
