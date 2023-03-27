package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResultSet;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectHandleFailoverException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * <p>TDSQL专属，直连模式备库故障转移处理器</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectFailoverSlavesHandler implements TdsqlDirectFailoverHandler {

    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final TdsqlDirectScheduleServer scheduleServer;
    private final TdsqlDirectConnectionManager connectionManager;

    public TdsqlDirectFailoverSlavesHandler(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
        this.scheduleServer = dataSourceConfig.getScheduleServer();
        this.connectionManager = dataSourceConfig.getConnectionManager();
    }

    /**
     * 处理备库故障转移
     *
     * @param slaveResultSet 备库拓扑信息比较结果
     */
    @Override
    public void handleSlaves(SlaveResultSet<SlaveResult> slaveResultSet) {
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
        for (SlaveResult slaveResult : slaveResultSet) {
            TdsqlDirectTopologyChangeEventEnum changeEvent = slaveResult.getTdsqlDirectTopoChangeEventEnum();
            switch (changeEvent) {
                case SLAVE_ONLINE:
                    // 处理上线
                    this.handleOnline(slaveResult.getNewSlaveSet());
                    break;
                case SLAVE_OFFLINE:
                    // 处理下线
                    this.handleOffline(slaveResult.getOldSlaveSet());
                    break;
                case SLAVE_DELAY_CHANGE:
                case SLAVE_WEIGHT_CHANGE:
                case SLAVE_ALL_ATTR_CHANGE:
                    // 处理属性变化
                    this.handleAttributeChange(slaveResult.getAttributeChangedMap());
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
    private void handleOnline(Set<TdsqlDirectSlaveTopologyInfo> slaveSet) {
        // 过滤
        slaveSet = this.doFilterSet(slaveSet);
        if (slaveSet.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.OnlineSlaveSetIsEmpty"));
            return;
        }

        // 上线备库加入调度
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            this.scheduleServer.addSlave(directHostInfo);
        }
    }

    /**
     * 处理下线
     *
     * @param slaveSet 下线备库拓扑信息集合
     */
    private void handleOffline(Set<TdsqlDirectSlaveTopologyInfo> slaveSet) {
        if (slaveSet.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.OfflineSlaveSetIsEmpty"));
            return;
        }

        // 下线备库移除调度，并关闭所有存量连接
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            this.scheduleServer.removeSlave(directHostInfo);
            this.connectionManager.closeAllConnection(directHostInfo);
        }
    }

    /**
     * 处理属性变化
     *
     * @param attributeChangedMap 属性变化备库拓扑信息映射
     */
    private void handleAttributeChange(
            Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> attributeChangedMap) {
        // 过滤
        DoFilterMapResult doFilterMapResult = this.doFilterMap(attributeChangedMap);
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap = doFilterMapResult.getValidMap();
        Set<TdsqlDirectSlaveTopologyInfo> invalidSet = doFilterMapResult.getInvalidSet();

        // 当属性变化时，被过滤掉的备库信息目前有两种情况：
        // 1.延迟时间持续增长达到设置的最大阈值
        // 2.权重值变为小于等于零
        // 因此需要将被过滤掉的备库从调度中移除
        if (!invalidSet.isEmpty()) {
            for (TdsqlDirectSlaveTopologyInfo invalidTopologyInfo : invalidSet) {
                TdsqlDirectHostInfo directHostInfo = invalidTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
                // 仅移除调度，并不关闭存量连接
                this.scheduleServer.removeSlave(directHostInfo);
            }
        }

        if (validMap.isEmpty()) {
            TdsqlLoggerFactory.logWarn(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectHandleFailoverMessage.AttrChangeSlaveSetIsEmpty"));
            return;
        }

        // 属性变化备库更新调度
        for (Entry<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> entry : validMap.entrySet()) {
            TdsqlDirectHostInfo oldDirectHostInfo = entry.getKey().convertToDirectHostInfo(this.dataSourceConfig);
            TdsqlDirectHostInfo newDirectHostInfo = entry.getValue().convertToDirectHostInfo(this.dataSourceConfig);
            this.scheduleServer.updateSlave(oldDirectHostInfo, newDirectHostInfo);
        }
    }

    /**
     * 根据条件过滤掉无效备库集合信息
     *
     * @param sourceSet 备库拓扑信息集合
     * @return 过滤后的有效的备库拓扑信息集合
     */
    private Set<TdsqlDirectSlaveTopologyInfo> doFilterSet(Set<TdsqlDirectSlaveTopologyInfo> sourceSet) {
        Set<TdsqlDirectSlaveTopologyInfo> targetSet = new LinkedHashSet<>(sourceSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : sourceSet) {
            if (this.shouldBeFiltered(slaveTopologyInfo)) {
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
    private DoFilterMapResult doFilterMap(Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> sourceMap) {
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> validMap = new LinkedHashMap<>(
                sourceMap.keySet().size());
        Set<TdsqlDirectSlaveTopologyInfo> invalidSet = new LinkedHashSet<>(sourceMap.keySet().size());

        for (Entry<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> entry : sourceMap.entrySet()) {
            TdsqlDirectSlaveTopologyInfo oldTopologyInfo = entry.getKey();
            TdsqlDirectSlaveTopologyInfo newTopologyInfo = entry.getValue();
            if (this.shouldBeFiltered(newTopologyInfo)) {
                // 注意，这里加入的是已调度的备库拓扑信息
                invalidSet.add(oldTopologyInfo);
                continue;
            }
            validMap.put(oldTopologyInfo, newTopologyInfo);
        }
        return new DoFilterMapResult(validMap, invalidSet);
    }

    /**
     * 判断备库信息是否无效应该被过滤
     *
     * @param slaveTopologyInfo 备库拓扑信息
     * @return 如果无效需要被过滤返回 {@code true}，否则返回 {@code false}
     */
    private boolean shouldBeFiltered(TdsqlDirectSlaveTopologyInfo slaveTopologyInfo) {
        Integer maxSlaveDelaySeconds = this.dataSourceConfig.getTdsqlDirectMaxSlaveDelaySeconds();

        // 如果配置了备库最大延迟，并且待过滤的拓扑信息大于该值时，需要被过滤，同时打印警告日志
        if (!Objects.equals(maxSlaveDelaySeconds, DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS)
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
