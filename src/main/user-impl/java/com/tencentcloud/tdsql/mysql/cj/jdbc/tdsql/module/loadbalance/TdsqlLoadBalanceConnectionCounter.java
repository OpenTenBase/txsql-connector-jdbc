package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.loadbalance;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logWarn;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.NodeMsg;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>TDSQL-MySQL专属的，负载均衡连接计数器</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLoadBalanceConnectionCounter {

    /**
     * 保存了每一个DataSourceUuid与其连接计数器的映射关系
     */
    private final Map<String, TdsqlAtomicLongMap<TdsqlHostInfo>> counterDatasourceMap = new HashMap<>();
    private final ReentrantReadWriteLock counterLock = new ReentrantReadWriteLock();

    private TdsqlLoadBalanceConnectionCounter() {
    }

    /**
     * <p>
     * 初始化方法，当发现新的DataSourceUuid时触发，
     * 每个DataSourceUuid都保有一个自己专属的全局连接计数器
     * </p>
     *
     * @param tdsqlLoadBalanceInfo {@link TdsqlLoadBalanceInfo} 负载均衡信息记录类对象
     */
    public synchronized void initialize(TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo) {
        logInfo("Connection counter initializing.");
        String datasourceUuid = tdsqlLoadBalanceInfo.getDatasourceUuid();
        if (!this.counterDatasourceMap.containsKey(datasourceUuid)) {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = TdsqlAtomicLongMap.create();
            for (TdsqlHostInfo tdsqlHostInfo : tdsqlLoadBalanceInfo.getTdsqlHostInfoList()) {
                // 在本类中，所有的put方法，都从之前put一个Long类型修改为一个NodeMsg实例，
                // 因为isMaster字段在该功能中不重要，所以将其设为null
                long heartbeatIntervalTime = tdsqlLoadBalanceInfo.getTdsqlLoadBalanceHeartbeatIntervalTimeMillis();
                tdsqlHostInfo.setHeartbeatIntervalTime(heartbeatIntervalTime);
                counter.put(tdsqlHostInfo, new NodeMsg(0L, null));
            }
            this.counterDatasourceMap.put(datasourceUuid, counter);
            logInfo("New datasource add in counter [" + datasourceUuid + "], current counter ["
                    + this.printCounter() + "]");
        }
        logInfo("Connection counter initialized, current counter [" + this.printCounter() + "]");
    }

    /**
     * <p>根据DataSourceUuid获取其对应的全局连接计数器</p>
     *
     * @param datasourceUuid 全局唯一的DataSourceUuid
     * @return 与DataSourceUuid对应的全局连接计数器
     */
    public TdsqlAtomicLongMap<TdsqlHostInfo> getCounter(String datasourceUuid) {
        this.counterLock.readLock().lock();
        try {
            if (!this.counterDatasourceMap.containsKey(datasourceUuid)) {
                return null;
            }
            TdsqlAtomicLongMap<TdsqlHostInfo> map = this.counterDatasourceMap.get(datasourceUuid);
            if (map == null) {
                return null;
            }
            TdsqlAtomicLongMap<TdsqlHostInfo> filteredCounter = TdsqlAtomicLongMap.create();
            map.asMap().forEach((k, v) -> {
                if (!TdsqlLoadBalanceBlacklistHolder.getInstance().inBlacklist(k)) {
                    filteredCounter.put(k, v);
                }
            });
            if (filteredCounter.isEmpty()) {
                return null;
            }
            return filteredCounter;
        } finally {
            this.counterLock.readLock().unlock();
        }
    }

    /**
     * <p>
     * 操作全局连接计数器，对某个IP地址进行增加计数操作
     * 如果需要增加计数的IP地址在全局连接计数器中不存在且不在黑名单中时，则对其进行赋值，并将计数器的值初始化为1，
     * 否则，将计数器的值加1
     * </p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要增加计数的IP地址
     */
    public void incrementCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            if (!counter.containsKey(tdsqlHostInfo)) {
                // 如果主机已加入黑名单，则不能再对其进行操作
                TdsqlLoadBalanceBlacklistHolder blacklistHolder = TdsqlLoadBalanceBlacklistHolder.getInstance();
                if (blacklistHolder.inBlacklist(tdsqlHostInfo)) {
                    logWarn("Host [" + tdsqlHostInfo.getHostPortPair()
                            + "] in blacklist, don't need increment, current counter [" + this.printCounter() + "]");
                } else {
                    counter.put(tdsqlHostInfo, new NodeMsg(1L, null));
                    logInfo("Increment counter to 1 success [" + tdsqlHostInfo.getHostPortPair()
                            + "], current counter [" + this.printCounter() + "]");
                }
            } else {
                NodeMsg nodeMsg = counter.incrementAndGet(tdsqlHostInfo);
                // 新增一个判空逻辑
                if (nodeMsg != null) {
                    long currCount = nodeMsg.getCount();
                    logInfo("Increment counter to " + currCount + " success [" + tdsqlHostInfo.getHostPortPair()
                            + "], current counter [" + this.printCounter() + "]");
                } else {
                    logInfo("The scheduleQueue doesn't have tdsqlHostInfo, current counter [" + this.printCounter()
                            + "]");
                }

            }
        } finally {
            this.counterLock.writeLock().unlock();
        }
    }

    /**
     * <p>
     * 操作全局连接计数器，对某个IP地址进行减少计数操作
     * 如果需要减少计数的IP地址在全局连接计数器中不存在，则对其进行赋值，并将计数器的值初始化为0，
     * 否则，将计数器的值减1，
     * 当计数器的值进行减少计数操作后的值小于零且不在黑名单中时，重置计数器的值为0
     * </p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要减少计数的IP地址
     */
    public void decrementCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            if (!counter.containsKey(tdsqlHostInfo)) {
                // 如果主机已加入黑名单，则不能再对其进行操作
                TdsqlLoadBalanceBlacklistHolder blacklistHolder = TdsqlLoadBalanceBlacklistHolder.getInstance();
                if (blacklistHolder.inBlacklist(tdsqlHostInfo)) {
                    logWarn("Host [" + tdsqlHostInfo.getHostPortPair()
                            + "] in blacklist, don't need decrement, current counter [" + this.printCounter() + "]");
                } else {
                    counter.put(tdsqlHostInfo, new NodeMsg(0L, null));
                    logInfo("Decrement counter to 0 success [" + tdsqlHostInfo.getHostPortPair()
                            + "], current counter [" + this.printCounter() + "]");
                }
            } else {
                NodeMsg nodeMsg = counter.decrementAndGet(tdsqlHostInfo);
                if (nodeMsg != null) {
                    long currCount = nodeMsg.getCount();
                    if (currCount < 0) {
                        counter.put(tdsqlHostInfo, new NodeMsg(0L, null));
                    }
                    logInfo("Decrement counter to " + currCount + " success [" + tdsqlHostInfo.getHostPortPair()
                            + "], current counter [" + this.printCounter() + "]");
                } else {
                    logInfo("The scheduleQueue doesn't have tdsqlHostInfo, current counter [" + this.printCounter()
                            + "]");
                }

            }
        } finally {
            this.counterLock.writeLock().unlock();
        }
    }

    /**
     * <p>
     * 重置某个IP地址的全局连接计数器
     * 无论该IP地址是否已经在全局连接计数器存在，都会进行赋值
     * 也无论当前计数器的值是多少，都会将计数器的值赋值为0
     * 已在黑名单中的不再进行以上操作
     * </p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要重置计数的IP地址
     */
    public void resetCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            // 如果主机已加入黑名单，则不能再对其进行操作
            //TdsqlLoadBalanceBlacklistHolder blacklistHolder = TdsqlLoadBalanceBlacklistHolder.getInstance();
            counter.put(tdsqlHostInfo, new NodeMsg(0L, null));
            logInfo("Reset counter to 0 success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                    + this.printCounter() + "]");

        } finally {
            this.counterLock.writeLock().unlock();
        }
    }

    /**
     * <p>删除某个IP地址的全局连接计数器</p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要删除计数的IP地址
     */
    public void removeCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            if (counter.containsKey(tdsqlHostInfo)) {
                counter.remove(tdsqlHostInfo);
                logInfo("Remove counter success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                        + this.printCounter() + "]");
            } else {
                logWarn("Host [" + tdsqlHostInfo.getHostPortPair() + "] not in counter, current counter ["
                        + this.printCounter() + "]");
            }
        } finally {
            this.counterLock.writeLock().unlock();
        }
    }

    public String printCounter() {
        this.counterLock.readLock().lock();
        try {
            StringBuilder print = new StringBuilder();
            for (Entry<String, TdsqlAtomicLongMap<TdsqlHostInfo>> entry : this.counterDatasourceMap.entrySet()) {
                String datasourceUuid = entry.getKey();
                TdsqlAtomicLongMap<TdsqlHostInfo> value = entry.getValue();
                StringJoiner hostCount = new StringJoiner(", ");
                for (Entry<TdsqlHostInfo, NodeMsg> mapEntry : value.asMap().entrySet()) {
                    String hostPortPair = mapEntry.getKey().getHostPortPair();
                    NodeMsg nodeMsg = mapEntry.getValue();
                    if (nodeMsg != null) {
                        Long count = nodeMsg.getCount();
                        hostCount.add(hostPortPair + "=" + count);
                    } else {
                        logInfo("the scheduleQueue doesn't have tdsqlHostInfo.");
                    }

                }
                print.append(datasourceUuid).append("{").append(hostCount).append("}");
            }
            return print.toString();
        } finally {
            this.counterLock.readLock().unlock();
        }
    }

    public ReentrantReadWriteLock getCounterLock() {
        return counterLock;
    }

    public static TdsqlLoadBalanceConnectionCounter getInstance() {
        return TdsqlLoadBalanceConnectionCounter.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlLoadBalanceConnectionCounter INSTANCE = new TdsqlLoadBalanceConnectionCounter();
    }
}
