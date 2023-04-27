package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.loadbalance;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logWarn;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>TDSQL-MySQL专属的，负载均衡黑名单寄存器类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLoadBalanceBlacklistHolder {

    private boolean blacklistEnabled = true;
    /**
     * 保存加入黑名单的IP地址信息集合
     */
    private final Set<TdsqlHostInfo> blacklist = new HashSet<>();
    private final ReentrantReadWriteLock blacklistLock = new ReentrantReadWriteLock();

    private TdsqlLoadBalanceBlacklistHolder() {
    }

    /**
     * <p>返回当前黑名单信息</p>
     *
     * @return 返回一个不可修改的黑名单集合
     */
    public Set<String> printBlacklist() {
        if (!this.blacklistEnabled) {
            return new HashSet<String>(1) {{
                add("Blacklist is not enabled.");
            }};
        }
        this.blacklistLock.readLock().lock();
        try {
            Set<String> cloneSet = new HashSet<>(this.blacklist.size());
            for (TdsqlHostInfo tdsqlHostInfo : this.blacklist) {
                cloneSet.add(tdsqlHostInfo.getHostPortPair());
            }
            return Collections.unmodifiableSet(cloneSet);
        } finally {
            this.blacklistLock.readLock().unlock();
        }
    }

    /**
     * <p>将指定的IP地址加入黑名单</p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo} 尝试键入黑名单的IP地址信息
     */
    public void addBlacklist(TdsqlHostInfo tdsqlHostInfo) {
        if (!this.blacklistEnabled) {
            return;
        }

        this.blacklistLock.writeLock().lock();
        try {
            // 如果黑名单中还没有该IP地址信息，则加入
            // 同时移除该IP地址在全局连接计数器中的计数器，并记录信息级别的日志
            if (!this.blacklist.contains(tdsqlHostInfo)) {
                this.blacklist.add(tdsqlHostInfo);
                logInfo("Add host [" + tdsqlHostInfo.getHostPortPair()
                        + "] to blacklist success and try remove it in counter, current blacklist [" + printBlacklist()
                        + "]");
                TdsqlLoadBalanceConnectionCounter.getInstance().removeCounter(tdsqlHostInfo);
            } else {
                logWarn("Add host [" + tdsqlHostInfo.getHostPortPair()
                        + "] to blacklist failed, because this host is already in blacklist , current blacklist ["
                        + printBlacklist() + "]");
            }
        } finally {
            this.blacklistLock.writeLock().unlock();
        }
    }

    /**
     * <p>将指定IP地址信息，将其从黑名单中移除</p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo}
     */
    public void removeBlacklist(TdsqlHostInfo tdsqlHostInfo) {
        if (!this.blacklistEnabled) {
            return;
        }

        this.blacklistLock.writeLock().lock();
        try {
            if (this.blacklist.contains(tdsqlHostInfo)) {
                this.blacklist.remove(tdsqlHostInfo);
                logInfo("Remove host [" + tdsqlHostInfo.getHostPortPair()
                        + "] from blacklist success and try reset it in counter, current blacklist [" + printBlacklist()
                        + "]");
                TdsqlLoadBalanceConnectionCounter.getInstance().resetCounter(tdsqlHostInfo);
            } else {
                logInfo("Don't need to remove host [" + tdsqlHostInfo.getHostPortPair()
                        + "] from blacklist, because its not in blacklist, current blacklist [" + printBlacklist()
                        + "]");
            }
        } finally {
            this.blacklistLock.writeLock().unlock();
        }
    }

    /**
     * <p>判断指定IP地址，是否已加入黑名单</p>
     *
     * @param tdsqlHostInfo {@link TdsqlHostInfo}
     * @return 已加入返回true，否则返回false
     */
    public boolean inBlacklist(TdsqlHostInfo tdsqlHostInfo) {
        if (!this.blacklistEnabled) {
            return false;
        }

        this.blacklistLock.readLock().lock();
        try {
            return this.blacklist.contains(tdsqlHostInfo);
        } finally {
            this.blacklistLock.readLock().unlock();
        }
    }

    public boolean isBlacklistEnabled() {
        return blacklistEnabled;
    }

    public void setBlacklistEnabled(boolean blacklistEnabled) {
        this.blacklistEnabled = blacklistEnabled;
    }

    public static TdsqlLoadBalanceBlacklistHolder getInstance() {
        return TdsqlLoadBalanceBlacklistHolder.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlLoadBalanceBlacklistHolder INSTANCE = new TdsqlLoadBalanceBlacklistHolder();
    }
}
