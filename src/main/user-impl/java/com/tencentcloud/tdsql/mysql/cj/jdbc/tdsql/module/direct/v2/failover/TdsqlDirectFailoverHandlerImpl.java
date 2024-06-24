package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectHandleFailoverException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;

import java.util.*;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SWITCH;

public class TdsqlDirectFailoverHandlerImpl implements TdsqlDirectFailoverHandler {
    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;

    public TdsqlDirectFailoverHandlerImpl(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
    }

    /**
     * 处理主库故障转移
     *
     * @param masterResult 主库拓扑信息比较结果
     */
    @Override
    public void handleMaster(TdsqlDirectTopologyCacheCompareResult.MasterResult masterResult) {
        // 无变化抛出异常
        if (masterResult.isNoChange()) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.NoChangeForMaster"));
        }

        if (masterResult.isFirstLoad()) {
            this.dataSourceConfig.getScheduleServer().addMaster(masterResult.getNewMaster().convertToDirectHostInfo(this.dataSourceConfig));
            return;
        }

        // 变化类型应该为主备切换，否则抛出异常
        if (!SWITCH.equals(masterResult.getTdsqlDirectTopoChangeEventEnum())) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.UnknownChangeEventForMaster"));
        }

        TdsqlDirectHostInfo oldMaster = masterResult.getOldMaster().convertToDirectHostInfo(this.dataSourceConfig);
        TdsqlDirectHostInfo newMaster = masterResult.getNewMaster().convertToDirectHostInfo(this.dataSourceConfig);

        // 调用调度服务，更新主库调度信息
        this.dataSourceConfig.getScheduleServer().updateMaster(oldMaster, newMaster);

        // 调用连接管理器，关闭所有老主库存量连接
        this.dataSourceConfig.getConnectionManager().asyncCloseAllConnection(oldMaster);
    }

    /**
     * 处理备库故障转移
     *
     * @param slaveResultSet 备库拓扑信息比较结果
     */
    @Override
    public void handleSlaves(TdsqlDirectTopologyCacheCompareResult.SlaveResultSet<TdsqlDirectTopologyCacheCompareResult.SlaveResult> slaveResultSet) {
        // 读写模式无需处理备库故障转移，抛出异常
        TdsqlDirectReadWriteModeEnum rwMode = dataSourceConfig.getTdsqlDirectReadWriteMode();
        if (!RO.equals(rwMode)) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.SlaveChangedNotInRoMode"));
        }

        // 备库比较结果如果无变化，抛出异常
        if (slaveResultSet.isNoChange()) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.NoChangeForSlave"));
        }

        // 依次比较备库变化并处理
        for (TdsqlDirectTopologyCacheCompareResult.SlaveResult slaveResult : slaveResultSet) {
            TdsqlDirectTopologyChangeEventEnum changeEvent = slaveResult.getTdsqlDirectTopoChangeEventEnum();
            switch (changeEvent) {
                case FIRST_LOAD:
                case SLAVE_ONLINE:
                    // 处理上线
                    this.handleSlaveOnline(slaveResult.getNewSlaveSet());
                    break;
                case SLAVE_OFFLINE:
                    // 处理下线
                    this.handleSlaveOffline(slaveResult.getOldSlaveSet());
                    break;
                case SLAVE_DELAY_CHANGE:
                case SLAVE_WEIGHT_CHANGE:
                case SLAVE_ALL_ATTR_CHANGE:
                    // 处理属性变化
                    this.handleSlaveAttributeChange(slaveResult.getAttributeChangedMap());
                    break;
                case NO_CHANGE:
                    throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                            Messages.getString("TdsqlDirectHandleFailoverException.NoChangeForSlave"));
                default:
                    throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                            Messages.getString("TdsqlDirectHandleFailoverException.UnknownChangeEventForSlave"));
            }
        }
    }

    /**
     * 处理上线
     *
     * @param slaveSet 上线备库拓扑信息集合
     */
    private void handleSlaveOnline(Set<TdsqlDirectSlaveTopologyInfo> slaveSet) {
        // 过滤
        slaveSet = this.doSlaveFilterSet(slaveSet);
        if (slaveSet.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.OnlineSlaveSetIsEmpty"));
            return;
        }

        // 上线备库加入调度
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            this.dataSourceConfig.getScheduleServer().addSlave(directHostInfo);
        }
    }

    /**
     * 处理下线
     *
     * @param slaveSet 下线备库拓扑信息集合
     */
    private void handleSlaveOffline(Set<TdsqlDirectSlaveTopologyInfo> slaveSet) {
        if (slaveSet.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.OfflineSlaveSetIsEmpty"));
            return;
        }

        // 下线备库移除调度，并关闭所有存量连接
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            TdsqlLoggerFactory.logInfo(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.OfflineSlaveHost",
                    new Object[]{directHostInfo.getHostPortPair(), "it has been offline"}));
            this.dataSourceConfig.getScheduleServer().removeSlave(directHostInfo);
            this.dataSourceConfig.getConnectionManager().asyncCloseAllConnection(directHostInfo);
        }
    }

    /**
     * 处理属性变化
     *
     * @param attributeChangedMap 属性变化备库拓扑信息映射
     */
    private void handleSlaveAttributeChange(
            Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> attributeChangedMap) {
        if (attributeChangedMap.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.AttrChangeSlaveSetIsEmpty"));
            return;
        }
        // 过滤
        DoFilterMapResult doFilterMapResult = this.doSlaveFilterMap(attributeChangedMap);
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap = doFilterMapResult.getValidMap();
        Set<TdsqlDirectSlaveTopologyInfo> invalidSet = doFilterMapResult.getInvalidSet();

        // 当属性变化时，被过滤掉的备库信息目前有两种情况：
        // 1.延迟时间持续增长达到设置的最大阈值
        // 2.权重值变为小于等于零
        // 因此需要将被过滤掉的备库从调度中移除
        if (!invalidSet.isEmpty()) {
            for (TdsqlDirectSlaveTopologyInfo invalidTopologyInfo : invalidSet) {
                TdsqlDirectHostInfo directHostInfo = invalidTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
                // 移除调度，关闭存量连接
                TdsqlLoggerFactory.logInfo(this.dataSourceUuid,
                        Messages.getString("TdsqlDirectHandleFailoverMessage.OfflineSlaveHost",
                                new Object[]{directHostInfo.getHostPortPair(), "its attributes has been invalid!"}));
                this.dataSourceConfig.getScheduleServer().removeSlave(directHostInfo);
                this.dataSourceConfig.getConnectionManager().asyncCloseAllConnection(directHostInfo);
            }
        }

        // 属性变化备库更新调度
        for (Map.Entry<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> entry : validMap.entrySet()) {
            TdsqlDirectHostInfo oldDirectHostInfo = entry.getKey().convertToDirectHostInfo(this.dataSourceConfig);
            TdsqlDirectHostInfo newDirectHostInfo = entry.getValue().convertToDirectHostInfo(this.dataSourceConfig);
            this.dataSourceConfig.getScheduleServer().updateSlave(oldDirectHostInfo, newDirectHostInfo);
        }
    }

    /**
     * 根据条件过滤掉无效备库集合信息
     *
     * @param sourceSet 备库拓扑信息集合
     * @return 过滤后的有效的备库拓扑信息集合
     */
    private Set<TdsqlDirectSlaveTopologyInfo> doSlaveFilterSet(Set<TdsqlDirectSlaveTopologyInfo> sourceSet) {
        Set<TdsqlDirectSlaveTopologyInfo> targetSet = new LinkedHashSet<>(sourceSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : sourceSet) {
            if (this.shouldSlaveBeFiltered(slaveTopologyInfo)) {
                continue;
            }
            targetSet.add(slaveTopologyInfo);
        }
        return targetSet;
    }

    /**
     * 根据条件过滤掉无效备库映射信息
     *
     * @param sourceMap 备库拓扑信息映射
     * @return {@link DoFilterMapResult} 包含有效拓扑信息映射和无效拓扑信息集合
     */
    private DoFilterMapResult doSlaveFilterMap(Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> sourceMap) {
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap = new LinkedHashMap<>(
                sourceMap.keySet().size());
        Set<TdsqlDirectSlaveTopologyInfo> invalidSet = new LinkedHashSet<>(sourceMap.keySet().size());

        for (Map.Entry<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> entry : sourceMap.entrySet()) {
            TdsqlDirectSlaveTopologyInfo oldTopologyInfo = entry.getKey();
            TdsqlDirectSlaveTopologyInfo newTopologyInfo = entry.getValue();
            if (this.shouldSlaveBeFiltered(newTopologyInfo)) {
                // 注意，这里加入的是已调度的备库拓扑信息
                invalidSet.add(oldTopologyInfo);
                continue;
            }
            if (this.shouldBeIgnored(oldTopologyInfo, newTopologyInfo)) {
                continue;
            }
            validMap.put(oldTopologyInfo, newTopologyInfo);
        }
        return new DoFilterMapResult(validMap, invalidSet);
    }

    private boolean shouldBeIgnored(TdsqlDirectSlaveTopologyInfo oldTopoInfo, TdsqlDirectSlaveTopologyInfo newTopoInfo) {
        // 如果权重变了，则不可能呢忽略
        if (!oldTopoInfo.getWeight().equals(newTopoInfo.getWeight())) {
            return false;
        }

        // 如果用户没设置最大延迟阈值，则可以直接忽略delay变化
        Integer maxSlaveDelaySeconds = this.dataSourceConfig.getTdsqlDirectMaxSlaveDelaySeconds();
        if (Objects.equals(maxSlaveDelaySeconds, DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS)) {
            return true;
        }

        // 如果最新和上一次的延迟都在阈值内，则该属性变化可以直接忽略
        if (newTopoInfo.getDelay() < maxSlaveDelaySeconds && oldTopoInfo.getDelay() < maxSlaveDelaySeconds) {
            return true;
        }
        return false;
    }

    /**
     * 判断备库信息是否无效应该被过滤
     *
     * @param slaveTopologyInfo 备库拓扑信息
     * @return 如果无效需要被过滤返回 {@code true}，否则返回 {@code false}
     */
    private boolean shouldSlaveBeFiltered(TdsqlDirectSlaveTopologyInfo slaveTopologyInfo) {
        Integer maxSlaveDelaySeconds = this.dataSourceConfig.getTdsqlDirectMaxSlaveDelaySeconds();

        // 如果配置了备库最大延迟，并且待过滤的拓扑信息大于该值时，需要被过滤，同时打印警告日志
        if (maxSlaveDelaySeconds != null
                && !Objects.equals(maxSlaveDelaySeconds, DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS)
                && slaveTopologyInfo.getDelay() >= maxSlaveDelaySeconds) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.IgnoreSlaveForMaxSlaveDelay",
                            new Object[]{slaveTopologyInfo.printPretty()}));
            return true;
        }

        // 若果待过滤拓扑信息权重小于等于零，需要被过滤，同时打印警告日志
        if (slaveTopologyInfo.getWeight() <= 0) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.IgnoreSlaveForZeroWeightFactor",
                            new Object[]{slaveTopologyInfo.printPretty()}));
            return true;
        }
        return false;
    }

    /**
     * 过滤无效备库映射信息结果类
     */
    private static class DoFilterMapResult {

        private final Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap;
        private final Set<TdsqlDirectSlaveTopologyInfo> invalidSet;

        public DoFilterMapResult(Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap,
                                 Set<TdsqlDirectSlaveTopologyInfo> invalidSet) {
            this.validMap = validMap;
            this.invalidSet = invalidSet;
        }

        public Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> getValidMap() {
            return validMap;
        }

        public Set<TdsqlDirectSlaveTopologyInfo> getInvalidSet() {
            return invalidSet;
        }
    }
}
