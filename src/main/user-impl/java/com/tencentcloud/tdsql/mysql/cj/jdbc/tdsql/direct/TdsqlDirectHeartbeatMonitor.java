package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDirectHeartbeatAttributes;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TdsqlDirectHeartbeatMonitor {
    private boolean heartbeatMonitorInitialized;
    private ScheduledThreadPoolExecutor heartbeatMonitor;       //线程池
    private Set<TdsqlHostInfo> monitoredTdsqlHostInfoSet;       //每个监测过的节点放进这个set中！

    private TdsqlDirectHeartbeatMonitor() {
        this.heartbeatMonitorInitialized = false;
    }

    /**
     * 只需要初始化一次，但是需要心跳的相关参数，以及每个节点信息
     * @param scheduleQueue
     * @param tdsqlDirectHeartbeatAttributes
     */
    public void initialize(TdsqlAtomicLongMap scheduleQueue, TdsqlDirectHeartbeatAttributes tdsqlDirectHeartbeatAttributes) {
        if (!this.heartbeatMonitorInitialized) {
            TdsqlLoggerFactory.logInfo("Heartbeat monitor initializing.");
            this.heartbeatMonitor = new ScheduledThreadPoolExecutor(2, (new TdsqlThreadFactoryBuilder()).setDaemon(true).setNameFormat("Heartbeat-pool-%d").build());
            this.monitoredTdsqlHostInfoSet = new HashSet();
            this.heartbeatMonitorInitialized = true;
            TdsqlLoggerFactory.logInfo("Heartbeat monitor initialized.");
        }

        //针对每一个节点进行
        Iterator var3 = scheduleQueue.asMap().keySet().iterator();

        while(var3.hasNext()) {
            TdsqlHostInfo tdsqlHostInfo = (TdsqlHostInfo)var3.next();
            if (!this.monitoredTdsqlHostInfoSet.contains(tdsqlHostInfo)) {
                this.heartbeatMonitor.scheduleWithFixedDelay(new TdsqlDirectHeartbeatMonitor.HeartbeatMonitorTask(tdsqlHostInfo,
                        tdsqlDirectHeartbeatAttributes.getTdsqlLoadDirectHeartbeatMaxErrorRetries(),
                        tdsqlDirectHeartbeatAttributes.getTdsqlDirectHeartbeatErrorRetryIntervalTimeMillis()), 0L, (long)tdsqlDirectHeartbeatAttributes.getTdsqlDirectHeartbeatIntervalTimeMillis(),
                        TimeUnit.MILLISECONDS);
                this.monitoredTdsqlHostInfoSet.add(tdsqlHostInfo);
            }
        }

    }

    private static class HeartbeatMonitorTask implements Runnable {
        private final TdsqlHostInfo tdsqlHostInfo;
        private final int retries;//重试次数
        private final int retryIntervalMs;  //间隔

        public HeartbeatMonitorTask(TdsqlHostInfo tdsqlHostInfo, int retries, int retryIntervalMs) {
            this.tdsqlHostInfo = tdsqlHostInfo;
            this.retries = retries;
            this.retryIntervalMs = retryIntervalMs;
        }

        public void run() {
            try {
                TdsqlLoggerFactory.logInfo("Start heartbeat monitor check [" + this.tdsqlHostInfo.getHostPortPair() + "]");
                int attemptCount = 0;
                Properties properties = this.tdsqlHostInfo.exposeAsProperties();
                properties.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
                properties.setProperty(PropertyKey.connectTimeout.getKeyName(), "1000");
                Map<String, String> map = new HashMap(properties.size());
                Iterator var4 = properties.entrySet().iterator();

                while(var4.hasNext()) {
                    Map.Entry<Object, Object> entry = (Map.Entry)var4.next();
                    map.put((String)entry.getKey(), (String)entry.getValue());
                }

                HostInfo heartbeatHostInfo = new HostInfo(this.tdsqlHostInfo.getOriginalUrl(),
                        this.tdsqlHostInfo.getHost(), this.tdsqlHostInfo.getPort(),
                        this.tdsqlHostInfo.getUser(), this.tdsqlHostInfo.getPassword(), map);
                for (int i = 0; i <= retries; i++) {
                    TdsqlLoggerFactory.logInfo(
                            "Start heartbeat monitor check, now attempts [" + attemptCount + "]. HostInfo ["
                                    + tdsqlHostInfo.getHostPortPair() + "]");
                    try (JdbcConnection connection = ConnectionImpl.getInstance(heartbeatHostInfo);
                         Statement stmt = connection.createStatement()) {
                        // 设置执行心跳检测SQL的超时时间为1秒
                        stmt.setQueryTimeout(1);
                        stmt.executeQuery("select 1");
                        // 程序执行到这里，表示心跳检测成功，尝试将该IP地址从黑名单中移除
                        // 但具体是否真的需要从黑名单移除，或者是否移除成功，取决于全局黑名单记录器
                        TdsqlDirectBlacklistHolder.getInstance().removeBlacklist(tdsqlHostInfo);
                        // 心跳检测成功记录调试级别日志，退出当前循环后，等待下次调度
                        TdsqlLoggerFactory.logInfo(
                                "Success heartbeat monitor check [" + tdsqlHostInfo.getHostPortPair() + "]");
                        break;
                    } catch (SQLException e) {
                        // 计算并比较心跳检测次数是否达到允许的最大次数
                        // 如果没有达到，则继续下次心跳检测
                        // 否则，将该IP地址加入黑名单并记录错误级别的日志，同时更新首次检测标识和计数器
                        if (attemptCount + 1 > retries) {
                            // 加入黑名单
                            TdsqlLoggerFactory.logError("Host heartbeat monitor failed. now attempts [" + attemptCount
                                    + "] equals max attempts [" + retries + "], try add to blacklist. HostInfo ["
                                    + tdsqlHostInfo.getHostPortPair() + "]", e);
                            TdsqlDirectBlacklistHolder.getInstance().addBlacklist(tdsqlHostInfo);
                        } else {
                            // 心跳检测失败处理逻辑，程序执行到这里有可能是建立连接失败、超时，或执行心跳检测SQL失败、超时。
                            // 无论是上述哪种情况，都需要记录错误级别日志
                            TdsqlLoggerFactory.logError(
                                    "Host heartbeat monitor failed and try again, now attempts [" + attemptCount
                                            + "], max attempts [" + retries + "]. HostInfo ["
                                            + tdsqlHostInfo.getHostPortPair() + "]", e);
                            // 间隔一段时间后再进行下一次尝试
                            if (this.retryIntervalMs > 0) {
                                TimeUnit.MILLISECONDS.sleep(this.retryIntervalMs);
                            }
                        }
                        ++attemptCount;
                    }
                }
            } catch (Exception e) {
                TdsqlLoggerFactory.logError("Host heartbeat monitor check error [" + this.tdsqlHostInfo.getHostPortPair() + "]", e);
            }

        }
    }

    public static TdsqlDirectHeartbeatMonitor getInstance() {
        return TdsqlDirectHeartbeatMonitor.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {
        private static final TdsqlDirectHeartbeatMonitor INSTANCE = new TdsqlDirectHeartbeatMonitor();

        private SingletonInstance() {
        }
    }
}
