package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_OFFLINE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_ONLINE;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.MasterResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResultSet;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCompareTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyInfo;

import java.util.*;

/**
 * <p>TDSQL专属，直连模式缓存比较器</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyCacheComparator {

    private final String dataSourceUuid;

    private final TdsqlDirectDataSourceConfig dataSourceConfig;

    public TdsqlDirectTopologyCacheComparator(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
    }

    /**
     * 比较主库拓扑信息
     *
     * @param cachedMaster 已缓存主库
     * @param newestMaster 待比较主库
     * @return {@link MasterResult} 比较结果
     */
    public MasterResult compareMaster(TdsqlDirectMasterTopologyInfo cachedMaster,
            TdsqlDirectMasterTopologyInfo newestMaster) {

        if (cachedMaster == null) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.NullCachedTopology"));
        }
        if (newestMaster == null) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.NullNewestTopology"));
        }

        // 无变化
        if (cachedMaster.equals(newestMaster)) {
            return MasterResult.noChange(this.dataSourceUuid);
        }
        // 有变化
        return MasterResult.switchMaster(this.dataSourceUuid, cachedMaster, newestMaster);
    }

    /**
     * 比较备库拓扑信息
     *
     * @param cachedSlaveSet 已缓存备库
     * @param newestSlaveSet 待比较备库
     * @return {@link SlaveResultSet}&lt;{@link SlaveResult}&gt; 比较结果
     */
    public SlaveResultSet<SlaveResult> compareSlaves(Set<TdsqlDirectSlaveTopologyInfo> cachedSlaveSet,
            Set<TdsqlDirectSlaveTopologyInfo> newestSlaveSet) {

        if (cachedSlaveSet == null) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.NullCachedTopology"));
        }
        if (newestSlaveSet == null) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.NullNewestTopology"));
        }

        // 使用 Host 和 Port 去重
        Set<TdsqlDirectSlaveTopologyInfo> distinctCachedSlaveSet = this.distinctByHostPortPair(cachedSlaveSet);
        if (distinctCachedSlaveSet.size() != cachedSlaveSet.size()) {
            StringJoiner joiner = new StringJoiner(",");
            for (TdsqlDirectSlaveTopologyInfo info : cachedSlaveSet) {
                joiner.add(info.printPretty());
            }
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.RepeatCachedTopology",
                            new Object[]{joiner.toString()}));
        }
        Set<TdsqlDirectSlaveTopologyInfo> distinctNewestSlaveSet = this.distinctByHostPortPair(newestSlaveSet);
        if (distinctNewestSlaveSet.size() != newestSlaveSet.size()) {
            StringJoiner joiner = new StringJoiner(",");
            for (TdsqlDirectSlaveTopologyInfo info : newestSlaveSet) {
                joiner.add(info.printPretty());
            }
            throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                    Messages.getString("TdsqlDirectCompareTopologyException.RepeatNewestTopology",
                            new Object[]{joiner.toString()}));
        }

        SlaveResultSet<SlaveResult> resultSet = new SlaveResultSet<>();

        if (distinctCachedSlaveSet.isEmpty()) {
            if (distinctNewestSlaveSet.isEmpty()) {
                resultSet.add(SlaveResult.noChange(this.dataSourceUuid));
            } else {
                resultSet.add(SlaveResult.onlineSlave(this.dataSourceUuid, distinctNewestSlaveSet));
            }
            return resultSet;
        } else if (distinctNewestSlaveSet.isEmpty()) {
            resultSet.add(SlaveResult.offlineSlave(this.dataSourceUuid, distinctCachedSlaveSet));
            return resultSet;
        } else if (distinctCachedSlaveSet.equals(distinctNewestSlaveSet)) {
            resultSet.add(SlaveResult.noChange(this.dataSourceUuid));
            return resultSet;
        }

        resultSet.addAll(compareTwoSet(distinctCachedSlaveSet, distinctNewestSlaveSet));
//        // 判断是否有从库下线或属性变更
//        tempResultSet = this.compareInDeep(SLAVE_OFFLINE, distinctCachedSlaveSet, distinctNewestSlaveSet);
//        if (!tempResultSet.isEmpty()) {
//            resultSet.addAll(tempResultSet);
//        }
//
//        // 判断是否有从库上线或属性变更
//        tempResultSet = this.compareInDeep(SLAVE_ONLINE, distinctNewestSlaveSet, distinctCachedSlaveSet);
//        if (!tempResultSet.isEmpty()) {
//            resultSet.addAll(tempResultSet);
//        }

        return resultSet;
    }

    /**
     * 去重
     *
     * @param sourceSet {@link Set}&lt;{@link TdsqlDirectSlaveTopologyInfo}&gt;
     * @return 去重后的集合
     */
    private Set<TdsqlDirectSlaveTopologyInfo> distinctByHostPortPair(Set<TdsqlDirectSlaveTopologyInfo> sourceSet) {
        Set<String> existSet = new HashSet<>(sourceSet.size());
        Set<TdsqlDirectSlaveTopologyInfo> targetSet = new LinkedHashSet<>();

        for (TdsqlDirectSlaveTopologyInfo set : sourceSet) {
            String hostPortPair = set.getHostPortPair();
            if (existSet.contains(hostPortPair)) {
                continue;
            }
            targetSet.add(set);
            existSet.add(hostPortPair);
        }
        return targetSet;
    }

    /**
     * 深拷贝
     *
     * @param sourceSet 源集合
     * @return 深拷贝后的新副本
     */
    private Set<TdsqlDirectSlaveTopologyInfo> deepClone(Set<TdsqlDirectSlaveTopologyInfo> sourceSet) {
        LinkedHashSet<TdsqlDirectSlaveTopologyInfo> tdsqlDirectSlaveTopologyInfos = new LinkedHashSet<>(
                sourceSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopoInfo : sourceSet) {
            tdsqlDirectSlaveTopologyInfos.add(
                    new TdsqlDirectSlaveTopologyInfo(slaveTopoInfo.getDatasourceUuid(), slaveTopoInfo.getIp(),
                            slaveTopoInfo.getPort(), slaveTopoInfo.getWeight(), slaveTopoInfo.getIsWatch(),
                            slaveTopoInfo.getDelay()));
        }
        return tdsqlDirectSlaveTopologyInfos;
    }

    private Map<String, TdsqlDirectSlaveTopologyInfo> transferTopoInfoSetToMap(Set<TdsqlDirectSlaveTopologyInfo> topoInfoSet) {
        Map<String, TdsqlDirectSlaveTopologyInfo> topoInfoMap = new HashMap<>(topoInfoSet.size());
        for (TdsqlDirectSlaveTopologyInfo topoInfo : topoInfoSet) {
            topoInfoMap.put(topoInfo.getHostPortPair(), topoInfo);
        }

        return topoInfoMap;
    }

    private SlaveResultSet<SlaveResult> compareTwoSet(Set<TdsqlDirectSlaveTopologyInfo> oldSet, Set<TdsqlDirectSlaveTopologyInfo> newSet) {
        SlaveResultSet<SlaveResult> resultSet = new SlaveResultSet<>();
        Map<String, TdsqlDirectSlaveTopologyInfo> oldTopoInfoMap = transferTopoInfoSetToMap(oldSet);
        Map<String, TdsqlDirectSlaveTopologyInfo> newTopoInfoMap = transferTopoInfoSetToMap(newSet);
        // 计算出下线topo节点
        Set<TdsqlDirectSlaveTopologyInfo> offlineSet = new LinkedHashSet<>();
        Map<String, TdsqlDirectSlaveTopologyInfo> stillExistedTopoInfo = new LinkedHashMap<>();
        oldTopoInfoMap.forEach((k, v) -> {
            if (!newTopoInfoMap.containsKey(k)) {
                offlineSet.add(v);
            } else {
                stillExistedTopoInfo.put(k, v);
            }
        });
        if(offlineSet.size() != 0) {
            resultSet.add(SlaveResult.offlineSlave(this.dataSourceUuid, offlineSet));
        }

        // 计算出新上线节点以及属性变化节点
        Set<TdsqlDirectSlaveTopologyInfo> onlineSet = new LinkedHashSet<>();
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> allAttributeChangedMap = new LinkedHashMap<>();
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> weightChangedMap = new LinkedHashMap<>();
        Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> delayChangedMap = new LinkedHashMap<>();
        newTopoInfoMap.forEach((k, v) -> {
            if (!oldTopoInfoMap.containsKey(k)) {
                onlineSet.add(v);
            } else {
                TdsqlDirectSlaveTopologyInfo oldTopoInfo = stillExistedTopoInfo.get(k);
                if (!oldTopoInfo.getDelay().equals(v.getDelay()) &&
                        !oldTopoInfo.getWeight().equals(v.getWeight())) {
                    allAttributeChangedMap.put(oldTopoInfo, v);
                } else if (!oldTopoInfo.getDelay().equals(v.getDelay())) {
                    delayChangedMap.put(oldTopoInfo, v);
                } else if (!oldTopoInfo.getWeight().equals(v.getWeight())) {
                    weightChangedMap.put(oldTopoInfo, v);
                }
            }
        });
        if (onlineSet.size() != 0) {
            resultSet.add(SlaveResult.onlineSlave(this.dataSourceUuid, onlineSet));
        }
        if (allAttributeChangedMap.size() != 0) {
            resultSet.add(SlaveResult.changeAllAttr(this.dataSourceUuid, allAttributeChangedMap));
        }
        if (weightChangedMap.size() != 0) {
            resultSet.add(SlaveResult.changeWeight(this.dataSourceUuid, weightChangedMap));
        }
        if (delayChangedMap.size() != 0) {
            resultSet.add(SlaveResult.changeDelay(this.dataSourceUuid, delayChangedMap));
        }
        return resultSet;
    }

    /**
     * 深度比较
     *
     * @param changeEventEnum {@link TdsqlDirectTopologyChangeEventEnum} 拓扑变化事件枚举类
     * @param sourceSet 源集合
     * @param targetSet 目标集合
     * @return 比较结果
     */
    private SlaveResultSet<SlaveResult> compareInDeep(TdsqlDirectTopologyChangeEventEnum changeEventEnum,
            Set<TdsqlDirectSlaveTopologyInfo> sourceSet, Set<TdsqlDirectSlaveTopologyInfo> targetSet) {
        SlaveResultSet<SlaveResult> resultSet = new SlaveResultSet<>();

        Set<TdsqlDirectSlaveTopologyInfo> tempSourceSet = this.deepClone(sourceSet);
        tempSourceSet.removeAll(targetSet);
        if (tempSourceSet.isEmpty()) {
            resultSet.add(SlaveResult.noChange(this.dataSourceUuid));
        } else {
            Set<TdsqlDirectSlaveTopologyInfo> changeSet = new LinkedHashSet<>();
            Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> weightChangedMap = new LinkedHashMap<>();
            Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> delayChangedMap = new LinkedHashMap<>();

            for (TdsqlDirectSlaveTopologyInfo source : tempSourceSet) {
                String hostPortPair = source.getHostPortPair();
                Integer weight = source.getWeight();
                Integer delay = source.getDelay();

                boolean attributeChanged = false;

                for (TdsqlDirectSlaveTopologyInfo target : targetSet) {
                    if (hostPortPair.equals(target.getHostPortPair())) {
                        if (!weight.equals(target.getWeight())) {
                            attributeChanged = true;
                            if (SLAVE_ONLINE.equals(changeEventEnum)) {
                                weightChangedMap.put(target, source);
                            } else if (SLAVE_OFFLINE.equals(changeEventEnum)) {
                                weightChangedMap.put(source, target);
                            }
                        }
                        // 如果从等延迟没有超过阈值，或者没有阈值设定，
                        // 那么该节点等延迟改变可以不考虑在内，从而减少schedule改变的次数
//                        Integer maxSlaveDelaySeconds = this.dataSourceConfig.getTdsqlDirectMaxSlaveDelaySeconds();
//                        Boolean shouldSkip = !(maxSlaveDelaySeconds != null
//                                && !Objects.equals(maxSlaveDelaySeconds, DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS)
//                                && target.getDelay() >= maxSlaveDelaySeconds);
                        if (!delay.equals(target.getDelay())) {
                            attributeChanged = true;
                            if (SLAVE_ONLINE.equals(changeEventEnum)) {
                                delayChangedMap.put(target, source);
                            } else if (SLAVE_OFFLINE.equals(changeEventEnum)) {
                                delayChangedMap.put(source, target);
                            }
                        }
                        break;
                    }
                }
                if (!attributeChanged) {
                    changeSet.add(source);
                }
            }
            if (!changeSet.isEmpty()) {
                if (SLAVE_ONLINE.equals(changeEventEnum)) {
                    resultSet.add(SlaveResult.onlineSlave(this.dataSourceUuid, changeSet));
                } else if (SLAVE_OFFLINE.equals(changeEventEnum)) {
                    resultSet.add(SlaveResult.offlineSlave(this.dataSourceUuid, changeSet));
                } else {
                    throw TdsqlExceptionFactory.createException(TdsqlDirectCompareTopologyException.class,
                            Messages.getString("TdsqlDirectCompareTopologyException.UnknownChangedEvent"));
                }
            }

            if (!weightChangedMap.isEmpty() && !delayChangedMap.isEmpty()) {
                Set<TdsqlDirectSlaveTopologyInfo> weightChangedKeySet = this.deepClone(weightChangedMap.keySet());
                Set<TdsqlDirectSlaveTopologyInfo> delayChangedKeySet = this.deepClone(delayChangedMap.keySet());
                weightChangedKeySet.retainAll(delayChangedKeySet);

                if (!weightChangedKeySet.isEmpty()) {
                    Map<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo> allAttrChangedMap = new LinkedHashMap<>(
                            weightChangedKeySet.size());
                    for (TdsqlDirectSlaveTopologyInfo same : weightChangedKeySet) {
                        allAttrChangedMap.put(same, weightChangedMap.get(same));
                        weightChangedMap.remove(same);
                        delayChangedMap.remove(same);
                    }
                    resultSet.add(SlaveResult.changeAllAttr(this.dataSourceUuid, allAttrChangedMap));
                }
            }
            if (!weightChangedMap.isEmpty()) {
                resultSet.add(SlaveResult.changeWeight(this.dataSourceUuid, weightChangedMap));
            }
            if (!delayChangedMap.isEmpty()) {
                resultSet.add(SlaveResult.changeDelay(this.dataSourceUuid, delayChangedMap));
            }
        }
        return resultSet;
    }
}
