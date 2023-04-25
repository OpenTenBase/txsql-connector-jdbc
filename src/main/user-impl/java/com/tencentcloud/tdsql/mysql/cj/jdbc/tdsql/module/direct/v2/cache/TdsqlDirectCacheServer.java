package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EMPTY_STRING;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlInvalidConnectionPropertyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.MasterResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResultSet;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>TDSQL专属，直连模式缓存服务类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectCacheServer {

    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final String dataSourceUuid;
    private final TdsqlDirectTopologyCacheComparator cacheComparator;
    private final CountDownLatch finishedFirstCache;
    final ScheduledThreadPoolExecutor survivedChecker;
    private Boolean isInitialCached;
    private volatile Boolean isSurvived;
    private String clusterName;
    private TdsqlDirectTopologyInfo cachedTopologyInfo;
    private Long latestCachedTimeMillis;
    private Long latestComparedTimeMillis;

    /**
     * 构造方法
     *
     * @param dataSourceConfig 数据源配置信息
     */
    public TdsqlDirectCacheServer(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        // 初始化比较器
        this.cacheComparator = new TdsqlDirectTopologyCacheComparator(this.dataSourceConfig);
        // 确保调度服务已经初始化
        if (this.dataSourceConfig.getScheduleServer() == null) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                    Messages.getString("TdsqlDirectCacheTopologyException.NotCreatedScheduleServer"));
        }
        // 确保故障转移已经初始化
        if (this.dataSourceConfig.getFailoverHandler() == null) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                    Messages.getString("TdsqlDirectCacheTopologyException.NotCreatedFailoverHandler"));
        }
        this.finishedFirstCache = new CountDownLatch(1);
        this.isInitialCached = false;
        this.isSurvived = false;
        // 创建并启动缓存服务幸存状态检查器
        this.survivedChecker = new ScheduledThreadPoolExecutor(1, new TdsqlThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("SurvivedCheck-" + this.dataSourceUuid.substring(24, 32)).build());
        this.survivedChecker.scheduleWithFixedDelay(new TdsqlDirectSurvivedCheckTask(this), 0L,
                this.dataSourceConfig.getTdsqlDirectTopoRefreshIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    public void closeSurvivedChecker() {
        this.survivedChecker.shutdownNow();
    }

    /**
     * 比较、缓存拓扑信息
     *
     * @param topologyInfo {@link TdsqlDirectTopologyInfo}
     */
    public void compareAndCache(TdsqlDirectTopologyInfo topologyInfo) {
        TdsqlDirectValidateTopologyInfoResult validateResult = this.validateTopologyInfo(topologyInfo);
        if (!validateResult.isValidate) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectCacheTopologyException.class,
                    validateResult.errorMessage);
        }

        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();

        // 尚未初始化
        if (!this.isInitialCached && this.finishedFirstCache.getCount() == 1) {
            // 第一次缓存
            this.handleFirstCache(topologyInfo, rwMode);
            return;
        }

        TdsqlDirectMasterTopologyInfo masterTopologyInfo = topologyInfo.getMasterTopologyInfo();
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = topologyInfo.getSlaveTopologyInfoSet();

        // 比较主库、备库
        MasterResult masterResult = this.cacheComparator.compareMaster(
                this.cachedTopologyInfo.getMasterTopologyInfo(), masterTopologyInfo);
        SlaveResultSet<SlaveResult> slaveResultSet = this.cacheComparator.compareSlaves(
                this.cachedTopologyInfo.getSlaveTopologyInfoSet(), slaveTopologyInfoSet);

        // 主库、备库均无变化
        if (masterResult.isNoChange() && slaveResultSet.isNoChange()) {
            this.latestComparedTimeMillis = System.currentTimeMillis();
            TdsqlLoggerFactory.logDebug(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectCacheTopologyMessage.PrintCachedTopologyInfo",
                            new Object[]{this.cachedTopologyInfo.printPretty()}));
            return;
        }

        // 根据读写模式，分别处理主库、备库拓扑变化
        switch (rwMode) {
            case RW:
                // 读写模式，主库变化后，更新主库拓扑缓存，处理主库故障转移
                if (!masterResult.isNoChange()) {
                    this.cachedTopologyInfo.updateMaster(masterTopologyInfo);
                    this.dataSourceConfig.getFailoverHandler().handleMaster(masterResult);
                }

                if (!slaveResultSet.isNoChange()) {
                    this.cachedTopologyInfo.updateSlaveSet(slaveTopologyInfoSet);
                    TdsqlLoggerFactory.logDebug(this.dataSourceUuid,
                            Messages.getString("TdsqlDirectCacheTopologyMessage.SlaveChangedInRwMode"));
                    break;
                }
                break;
            case RO:
                if (!masterResult.isNoChange()) {
                    this.cachedTopologyInfo.updateMaster(masterTopologyInfo);
                    if (this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                        this.dataSourceConfig.getFailoverHandler().handleMaster(masterResult);
                    }
                }

                if (!slaveResultSet.isNoChange()) {
                    this.cachedTopologyInfo.updateSlaveSet(slaveTopologyInfoSet);
                    this.dataSourceConfig.getFailoverHandler().handleSlaves(slaveResultSet);
                }
                break;
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.createException(TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                                new Object[]{rwMode.getRwModeName()}));
        }
        this.latestComparedTimeMillis = System.currentTimeMillis();
    }

    /**
     * 等待第一次缓存完成的超时时间为60秒，超时抛出异常打印日志
     *
     * @return 未超时返回 {@code true}，否则返回 {@code false}
     */
    public boolean waitForFirstFinished() {
        try {
            if (!this.finishedFirstCache.await(this.dataSourceConfig.getTdsqlConnectionTimeOut(), TimeUnit.MILLISECONDS)) {
                if (this.dataSourceConfig.getTopologyServer().getRefreshTopologyTask().getLastException() != null) {
                    throw  TdsqlExceptionFactory.logException(this.dataSourceUuid,
                            TdsqlDirectCacheTopologyException.class,
                            Messages.getString("TdsqlDirectCacheTopologyException.FirstCacheTimeoutCauseBy",
                                    new Object[]{this.dataSourceConfig.getTdsqlConnectionTimeOut(),
                                            this.dataSourceConfig.getTopologyServer().getRefreshTopologyTask().getLastException().getMessage()}),
                            this.dataSourceConfig.getTopologyServer().getRefreshTopologyTask().getLastException());
                }
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                        Messages.getString("TdsqlDirectCacheTopologyException.FirstCacheTimeout", new Object[]{this.dataSourceConfig.getTdsqlConnectionTimeOut()}));
            }
        } catch (InterruptedException e) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCacheTopologyException.class,
                    Messages.getString("TdsqlDirectCacheTopologyException.FirstCacheInterrupted"));
        }
        return true;
    }

    /**
     * 校验拓扑信息
     *
     * @param topologyInfo {@link TdsqlDirectTopologyInfo}
     * @return {@link TdsqlDirectValidateTopologyInfoResult}
     */
    private TdsqlDirectValidateTopologyInfoResult validateTopologyInfo(TdsqlDirectTopologyInfo topologyInfo) {
        if (topologyInfo == null) {
            return TdsqlDirectValidateTopologyInfoResult.failure(
                    Messages.getString("TdsqlDirectCacheTopologyException.EmptyCachedTopologyInfo"));
        }
        if (!this.dataSourceUuid.equals(topologyInfo.getDataSourceUuid())) {
            return TdsqlDirectValidateTopologyInfoResult.failure(
                    Messages.getString("TdsqlDirectCacheTopologyException.DifferentDataSourceUuid",
                            new Object[]{this.dataSourceUuid, topologyInfo.getDataSourceUuid()}));
        }
        if (this.isInitialCached) {
            if (!this.clusterName.equals(topologyInfo.getClusterName())) {
                return TdsqlDirectValidateTopologyInfoResult.failure(
                        Messages.getString("TdsqlDirectCacheTopologyException.DifferentClusterName",
                                new Object[]{this.clusterName, topologyInfo.getClusterName()}));
            }
        }

        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = topologyInfo.getMasterTopologyInfo();
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = topologyInfo.getSlaveTopologyInfoSet();
        switch (rwMode) {
            case RW:
                if (masterTopologyInfo == null || !masterTopologyInfo.isValid()) {
                    return TdsqlDirectValidateTopologyInfoResult.failure(
                            Messages.getString("TdsqlDirectCacheTopologyException.InvalidMasterTopologyInfo",
                                    new Object[]{masterTopologyInfo}));
                }
                break;
            case RO:
                if (slaveTopologyInfoSet == null || slaveTopologyInfoSet.isEmpty()) {
                    if (this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                        if (masterTopologyInfo == null || !masterTopologyInfo.isValid()) {
                            return TdsqlDirectValidateTopologyInfoResult.failure(Messages.getString(
                                    "TdsqlDirectCacheTopologyException.InvalidMasterTopologyInfoWhenMasterAsSlave",
                                    new Object[]{masterTopologyInfo}));
                        }
                    } else {
                        return TdsqlDirectValidateTopologyInfoResult.failure(
                                Messages.getString("TdsqlDirectCacheTopologyException.InvalidSlaveTopologyInfo",
                                        new Object[]{slaveTopologyInfoSet}));
                    }
                }
                break;
            case UNKNOWN:
            default:
                return TdsqlDirectValidateTopologyInfoResult.failure(
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode"));
        }
        return TdsqlDirectValidateTopologyInfoResult.success();
    }

    /**
     * 第一次缓存
     *
     * @param topologyInfo {@link TdsqlDirectTopologyInfo}
     * @param rwMode {@link TdsqlDirectReadWriteModeEnum}
     */
    private void handleFirstCache(TdsqlDirectTopologyInfo topologyInfo, TdsqlDirectReadWriteModeEnum rwMode) {
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = topologyInfo.getMasterTopologyInfo();
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = topologyInfo.getSlaveTopologyInfoSet();
        switch (rwMode) {
            case RW:
                this.dataSourceConfig.getFailoverHandler().handleMaster(MasterResult.firstLoad(masterTopologyInfo));
                //this.scheduleServer.addMaster(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig));
                break;
            case RO:
                Set<TdsqlDirectSlaveTopologyInfo> newSlaveSet = new LinkedHashSet<>();
                for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
                    newSlaveSet.add(slaveTopologyInfo);
                    //this.scheduleServer.addSlave(slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig));
                }
                SlaveResultSet<SlaveResult> slaveResult = new SlaveResultSet<>();
                slaveResult.add(SlaveResult.firstLoad(newSlaveSet));
                this.dataSourceConfig.getFailoverHandler().handleSlaves(slaveResult);
                if (this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                    this.dataSourceConfig.getFailoverHandler().handleMaster(MasterResult.firstLoad(masterTopologyInfo));
                    //this.scheduleServer.addMaster(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig));
                }
                break;
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.createException(TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                                new Object[]{rwMode.getRwModeName()}));
        }
        this.clusterName = topologyInfo.getClusterName();
        this.cachedTopologyInfo = topologyInfo;
        this.latestCachedTimeMillis = System.currentTimeMillis();
        this.latestComparedTimeMillis = System.currentTimeMillis();
        this.isSurvived = false;
        this.isInitialCached = true;
        this.finishedFirstCache.countDown();
    }

    public TdsqlDirectDataSourceConfig getDataSourceConfig() {
        return dataSourceConfig;
    }

    public String getDataSourceUuid() {
        return dataSourceUuid;
    }

    public TdsqlDirectTopologyCacheComparator getCacheComparator() {
        return cacheComparator;
    }

    public TdsqlDirectScheduleServer getScheduleServer() {
        return dataSourceConfig.getScheduleServer();
    }

    public Boolean getSurvived() {
        return isSurvived;
    }

    public Boolean getInitialCached() {
        return isInitialCached;
    }

    public String getClusterName() {
        return clusterName;
    }

    public TdsqlDirectTopologyInfo getCachedTopologyInfo() {
        return cachedTopologyInfo;
    }

    public Long getLatestCachedTimeMillis() {
        return latestCachedTimeMillis;
    }

    /**
     * 缓存服务的幸存检查任务
     */
    private static class TdsqlDirectSurvivedCheckTask implements Runnable {

        private final TdsqlDirectCacheServer cacheServer;

        public TdsqlDirectSurvivedCheckTask(TdsqlDirectCacheServer cacheServer) {
            this.cacheServer = cacheServer;
        }

        @Override
        public void run() {
            try {
                // 没有初始化不检查
                if (!this.cacheServer.isInitialCached || this.cacheServer.latestCachedTimeMillis == null
                        || this.cacheServer.latestCachedTimeMillis <= 0) {
                    return;
                }

                // 配置的刷新拓扑间隔时间
                Integer intervalMillis = this.cacheServer.dataSourceConfig.getTdsqlDirectTopoRefreshIntervalMillis();

                // 如果连续三次没有缓存，进入幸存模式
                long currentTime = System.currentTimeMillis();
                this.cacheServer.isSurvived =
                        currentTime - this.cacheServer.latestComparedTimeMillis > intervalMillis * 3L;

                if (this.cacheServer.isSurvived) {
                    TdsqlLoggerFactory.logWarn(this.cacheServer.dataSourceUuid,
                            Messages.getString("TdsqlDirectCacheTopologyMessage.InSurvived"));
                }
            } catch (Exception e) {
                // Eat this exception
            }
        }
    }

    /**
     * 拓扑信息校验结果类
     */
    private static class TdsqlDirectValidateTopologyInfoResult {

        private final Boolean isValidate;
        private final String errorMessage;

        public static TdsqlDirectValidateTopologyInfoResult success() {
            return new TdsqlDirectValidateTopologyInfoResult(true, EMPTY_STRING);
        }

        public static TdsqlDirectValidateTopologyInfoResult failure(String errorMessage) {
            return new TdsqlDirectValidateTopologyInfoResult(false, errorMessage);
        }

        private TdsqlDirectValidateTopologyInfoResult(Boolean isValidate, String errorMessage) {
            this.isValidate = isValidate;
            this.errorMessage = errorMessage;
        }
    }
}
