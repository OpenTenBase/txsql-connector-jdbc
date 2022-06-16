package com.tencentcloud.tdsql.mysql.cj.jdbc;

import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.TdsqlLoadBalanceInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlLoggerFactory;
import java.util.HashMap;
import java.util.Map;
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
    public void initialize(TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo) {
        String datasourceUuid = tdsqlLoadBalanceInfo.getDatasourceUuid();
        if (!this.counterDatasourceMap.containsKey(datasourceUuid)) {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = TdsqlAtomicLongMap.create();
            for (TdsqlHostInfo tdsqlHostInfo : tdsqlLoadBalanceInfo.getTdsqlHostInfoList()) {
                counter.put(tdsqlHostInfo, 0L);
            }
            this.counterDatasourceMap.put(datasourceUuid, counter);
            TdsqlLoggerFactory.logInfo(
                    "New datasource add in counter [" + datasourceUuid + "], current counter ["
                            + this.counterDatasourceMap + "]");
        }
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
            return this.counterDatasourceMap.get(datasourceUuid);
        } finally {
            this.counterLock.readLock().unlock();
        }
    }

    /**
     * <p>
     * 操作全局连接计数器，对某个IP地址进行增加计数操作
     * 如果需要增加计数的IP地址在全局连接计数器中不存在，则对其进行赋值，并将计数器的值初始化为1，
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
                counter.put(tdsqlHostInfo, 1L);
                TdsqlLoggerFactory.logInfo(
                        "Increment counter to 1 success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                                + this.counterDatasourceMap + "]");
            } else {
                long currCount = counter.incrementAndGet(tdsqlHostInfo);
                TdsqlLoggerFactory.logInfo(
                        "Increment counter to " + currCount + " success [" + tdsqlHostInfo.getHostPortPair()
                                + "], current counter [" + this.counterDatasourceMap + "]");
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
     * 当计数器的值进行减少计数操作后的值小于零时，重置计数器的值为0
     * </p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要减少计数的IP地址
     */
    public void decrementCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            if (!counter.containsKey(tdsqlHostInfo)) {
                counter.put(tdsqlHostInfo, 0L);
                TdsqlLoggerFactory.logInfo(
                        "Decrement counter to 0 success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                                + this.counterDatasourceMap + "]");
            } else {
                long currCount = counter.decrementAndGet(tdsqlHostInfo);
                if (currCount < 0) {
                    counter.put(tdsqlHostInfo, 0L);
                }
                TdsqlLoggerFactory.logInfo(
                        "Decrement counter to " + currCount + " success [" + tdsqlHostInfo.getHostPortPair()
                                + "], current counter [" + this.counterDatasourceMap + "]");
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
     * </p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 需要重置计数的IP地址
     */
    public void resetCounter(TdsqlHostInfo tdsqlHostInfo) {
        this.counterLock.writeLock().lock();
        try {
            TdsqlAtomicLongMap<TdsqlHostInfo> counter = this.counterDatasourceMap.get(tdsqlHostInfo.getOwnerUuid());
            counter.put(tdsqlHostInfo, 0L);
            TdsqlLoggerFactory.logInfo(
                    "Reset counter to 0 success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                            + this.counterDatasourceMap + "]");
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
                TdsqlLoggerFactory.logInfo(
                        "Remove counter success [" + tdsqlHostInfo.getHostPortPair() + "], current counter ["
                                + this.counterDatasourceMap + "]");
            }
        } finally {
            this.counterLock.writeLock().unlock();
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
