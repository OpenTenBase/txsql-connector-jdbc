package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * <p>TDSQL专属，直连模式比较结果类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyCacheCompareResult {

    /**
     * 主库比较结果类
     */
    public static class MasterResult {

        private final TdsqlDirectTopologyChangeEventEnum tdsqlDirectTopologyChangeEventEnum;
        private final TdsqlDirectMasterTopologyInfo oldMaster;
        private final TdsqlDirectMasterTopologyInfo newMaster;
        private static final MasterResult NO_CHANGE_MASTER = new MasterResult(
                TdsqlDirectTopologyChangeEventEnum.NO_CHANGE, null, null);

        private MasterResult(TdsqlDirectTopologyChangeEventEnum tdsqlDirectTopologyChangeEventEnum,
                TdsqlDirectMasterTopologyInfo oldMaster,
                TdsqlDirectMasterTopologyInfo newMaster) {
            this.tdsqlDirectTopologyChangeEventEnum = tdsqlDirectTopologyChangeEventEnum;
            this.oldMaster = oldMaster;
            this.newMaster = newMaster;
        }

        /**
         * 无变化结果
         *
         * @param dataSourceUuid 数据源UUID
         * @return 比较结果
         */
        public static MasterResult noChange(String dataSourceUuid) {
            TdsqlLoggerFactory.logInfo(dataSourceUuid,
                    Messages.getString("TdsqlDirectCacheTopologyMessage.TopologyInfoNoChanged",
                            new Object[]{"MASTER"}));
            return NO_CHANGE_MASTER;
        }

        /**
         * 主备切换结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param oldMaster 老主库
         * @param newMaster 新主库
         * @return 比较结果
         */
        public static MasterResult switchMaster(String dataSourceUuid, TdsqlDirectMasterTopologyInfo oldMaster,
                TdsqlDirectMasterTopologyInfo newMaster) {
            TdsqlLoggerFactory.logInfo(dataSourceUuid,
                    Messages.getString("TdsqlDirectCacheTopologyMessage.MasterHasSwitch",
                            new Object[]{oldMaster.printPretty(), newMaster.printPretty()}));
            return new MasterResult(TdsqlDirectTopologyChangeEventEnum.SWITCH, oldMaster, newMaster);
        }

        /**
         * 是否无变化
         *
         * @return 无变化返回 {@code true}，否则返回 {@code false}
         */
        public boolean isNoChange() {
            return TdsqlDirectTopologyChangeEventEnum.NO_CHANGE.equals(this.tdsqlDirectTopologyChangeEventEnum);
        }

        public TdsqlDirectTopologyChangeEventEnum getTdsqlDirectTopoChangeEventEnum() {
            return tdsqlDirectTopologyChangeEventEnum;
        }

        public TdsqlDirectMasterTopologyInfo getOldMaster() {
            return oldMaster;
        }

        public TdsqlDirectMasterTopologyInfo getNewMaster() {
            return newMaster;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MasterResult masterResult = (MasterResult) o;
            return tdsqlDirectTopologyChangeEventEnum == masterResult.tdsqlDirectTopologyChangeEventEnum
                    && Objects.equals(
                    oldMaster, masterResult.oldMaster) && Objects.equals(newMaster, masterResult.newMaster);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tdsqlDirectTopologyChangeEventEnum, oldMaster, newMaster);
        }
    }

    /**
     * 备库比较结果类
     */
    public static class SlaveResult {

        private final TdsqlDirectTopologyChangeEventEnum tdsqlDirectTopologyChangeEventEnum;
        private final Set<TdsqlDirectSlaveTopologyInfo> oldSlaveSet;
        private final Set<TdsqlDirectSlaveTopologyInfo> newSlaveSet;
        private final Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> attributeChangedMap;
        private static final SlaveResult NO_CHANGE_SLAVE = new SlaveResult(TdsqlDirectTopologyChangeEventEnum.NO_CHANGE,
                null, null);

        private SlaveResult(TdsqlDirectTopologyChangeEventEnum tdsqlDirectTopologyChangeEventEnum,
                Set<TdsqlDirectSlaveTopologyInfo> oldSlaveSet,
                Set<TdsqlDirectSlaveTopologyInfo> newSlaveSet) {
            this.tdsqlDirectTopologyChangeEventEnum = tdsqlDirectTopologyChangeEventEnum;
            this.oldSlaveSet = oldSlaveSet;
            this.newSlaveSet = newSlaveSet;
            this.attributeChangedMap = null;
        }

        private SlaveResult(TdsqlDirectTopologyChangeEventEnum tdsqlDirectTopologyChangeEventEnum,
                Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> attributeChangedMap) {
            this.tdsqlDirectTopologyChangeEventEnum = tdsqlDirectTopologyChangeEventEnum;
            this.oldSlaveSet = null;
            this.newSlaveSet = null;
            this.attributeChangedMap = attributeChangedMap;
        }

        /**
         * 无变化结果
         *
         * @param dataSourceUuid 数据源UUID
         * @return 比较结果
         */
        public static SlaveResult noChange(String dataSourceUuid) {
            TdsqlLoggerFactory.logInfo(dataSourceUuid,
                    Messages.getString("TdsqlDirectCacheTopologyMessage.TopologyInfoNoChanged",
                            new Object[]{"SLAVES"}));
            return NO_CHANGE_SLAVE;
        }

        /**
         * 从库上线结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param onlineSlaveSet 上线备库拓扑信息集合
         * @return 比较结果
         */
        public static SlaveResult onlineSlave(String dataSourceUuid, Set<TdsqlDirectSlaveTopologyInfo> onlineSlaveSet) {
            if (onlineSlaveSet == null || onlineSlaveSet.isEmpty()) {
                return NO_CHANGE_SLAVE;
            }
            logOnlineOffline(dataSourceUuid, onlineSlaveSet, "TdsqlDirectCacheTopologyMessage.SlavesOnline");
            return new SlaveResult(TdsqlDirectTopologyChangeEventEnum.SLAVE_ONLINE, null, onlineSlaveSet);
        }

        /**
         * 从库下线结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param offlineSlaveSet 下线备库拓扑信息结合
         * @return 比较结果
         */
        public static SlaveResult offlineSlave(String dataSourceUuid,
                Set<TdsqlDirectSlaveTopologyInfo> offlineSlaveSet) {
            if (offlineSlaveSet == null || offlineSlaveSet.isEmpty()) {
                return NO_CHANGE_SLAVE;
            }
            logOnlineOffline(dataSourceUuid, offlineSlaveSet, "TdsqlDirectCacheTopologyMessage.SlavesOffline");
            return new SlaveResult(TdsqlDirectTopologyChangeEventEnum.SLAVE_OFFLINE, offlineSlaveSet, null);
        }

        /**
         * 备库权重变化结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param weightChangedMap 权重变化的备库拓扑信息映射
         * @return 比较结果
         */
        public static SlaveResult changeWeight(String dataSourceUuid,
                Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> weightChangedMap) {
            if (weightChangedMap == null || weightChangedMap.isEmpty()) {
                return NO_CHANGE_SLAVE;
            }
            logAttributeHasChanged(dataSourceUuid, weightChangedMap,
                    "TdsqlDirectCacheTopologyMessage.SlavesWeightHasChanged");
            return new SlaveResult(TdsqlDirectTopologyChangeEventEnum.SLAVE_WEIGHT_CHANGE, weightChangedMap);
        }

        /**
         * 备库延迟变化结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param delayChangedMap 延迟变化的备库拓扑信息映射
         * @return 比较结果
         */
        public static SlaveResult changeDelay(String dataSourceUuid,
                Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> delayChangedMap) {
            if (delayChangedMap == null || delayChangedMap.isEmpty()) {
                return NO_CHANGE_SLAVE;
            }
            logAttributeHasChanged(dataSourceUuid, delayChangedMap,
                    "TdsqlDirectCacheTopologyMessage.SlavesDelayHasChanged");
            return new SlaveResult(TdsqlDirectTopologyChangeEventEnum.SLAVE_DELAY_CHANGE, delayChangedMap);
        }

        /**
         * 权重、延迟均变化结果
         *
         * @param dataSourceUuid 数据源UUID
         * @param allAttrChangedMap 权重、延迟均变化的备库拓扑信息映射
         * @return 比较结果
         */
        public static SlaveResult changeAllAttr(String dataSourceUuid,
                Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> allAttrChangedMap) {
            if (allAttrChangedMap == null || allAttrChangedMap.isEmpty()) {
                return NO_CHANGE_SLAVE;
            }
            logAttributeHasChanged(dataSourceUuid, allAttrChangedMap,
                    "TdsqlDirectCacheTopologyMessage.SlavesAllAttrHasChanged");
            return new SlaveResult(TdsqlDirectTopologyChangeEventEnum.SLAVE_ALL_ATTR_CHANGE, allAttrChangedMap);
        }

        /**
         * 是否无变化
         *
         * @return 无变化返回 {@code true}，否则返回 {@code false}
         */
        public boolean isNoChange() {
            return TdsqlDirectTopologyChangeEventEnum.NO_CHANGE.equals(this.tdsqlDirectTopologyChangeEventEnum);
        }

        public TdsqlDirectTopologyChangeEventEnum getTdsqlDirectTopoChangeEventEnum() {
            return tdsqlDirectTopologyChangeEventEnum;
        }

        public Set<TdsqlDirectSlaveTopologyInfo> getOldSlaveSet() {
            return oldSlaveSet;
        }

        public Set<TdsqlDirectSlaveTopologyInfo> getNewSlaveSet() {
            return newSlaveSet;
        }

        public Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> getAttributeChangedMap() {
            return attributeChangedMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlaveResult slaveResult = (SlaveResult) o;
            return tdsqlDirectTopologyChangeEventEnum == slaveResult.tdsqlDirectTopologyChangeEventEnum
                    && Objects.equals(
                    oldSlaveSet, slaveResult.oldSlaveSet) && Objects.equals(newSlaveSet, slaveResult.newSlaveSet)
                    && Objects.equals(attributeChangedMap, slaveResult.attributeChangedMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tdsqlDirectTopologyChangeEventEnum, oldSlaveSet, newSlaveSet, attributeChangedMap);
        }

        /**
         * 上线或下线备库信息打印
         *
         * @param dataSourceUuid 数据源UUID
         * @param slaveSet 上线或下线的备库拓扑集合
         * @param messageKey 打印信息的主键
         */
        private static void logOnlineOffline(String dataSourceUuid, Set<TdsqlDirectSlaveTopologyInfo> slaveSet,
                String messageKey) {
            StringJoiner joiner = new StringJoiner(",");
            for (TdsqlDirectSlaveTopologyInfo info : slaveSet) {
                joiner.add(info.printPretty());
            }
            TdsqlLoggerFactory.logInfo(dataSourceUuid, Messages.getString(messageKey, new Object[]{joiner.toString()}));
        }

        /**
         * 属性变化备库信息打印
         *
         * @param dataSourceUuid 数据源UUID
         * @param changedMap 属性变化的备库拓扑信息映射
         * @param messageKey 打印信息的主键
         */
        private static void logAttributeHasChanged(String dataSourceUuid,
                Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> changedMap, String messageKey) {
            StringJoiner joiner = new StringJoiner(",");
            for (Entry<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> entry : changedMap.entrySet()) {
                joiner.add(String.format("%s -> %s", entry.getKey().printPretty(),
                        entry.getValue().printPretty()));
            }
            TdsqlLoggerFactory.logInfo(dataSourceUuid, Messages.getString(messageKey, new Object[]{joiner.toString()}));
        }
    }

    /**
     * 备库比较结果集合
     *
     * @param <T> {@link SlaveResult}
     */
    public static class SlaveResultSet<T extends SlaveResult> extends LinkedHashSet<T> {

        /**
         * 备库信息是否均无变化
         *
         * @return 均无变化返回 {@code true}，否则返回 {@code false}
         */
        public boolean isNoChange() {
            if (this.isEmpty()) {
                return true;
            }

            for (SlaveResult slaveResult : this) {
                if (!slaveResult.isNoChange()) {
                    return false;
                }
            }
            return true;
        }
    }
}
