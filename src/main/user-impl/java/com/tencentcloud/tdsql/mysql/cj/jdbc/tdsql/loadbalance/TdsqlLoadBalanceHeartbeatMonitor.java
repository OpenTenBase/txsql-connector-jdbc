package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>TDSQL-MySQL独有的，负载均衡心跳检测监视器类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLoadBalanceHeartbeatMonitor {

    /**
     * 标识心跳检测监视器的初始化状态
     */
    private boolean heartbeatMonitorInitialized = false;
    /**
     * 心跳检测监视器的调度任务类型的线程池
     */
    private ScheduledThreadPoolExecutor heartbeatMonitor;
    /**
     * 记录已经加入过心跳检测任务的IP地址
     */
    private Set<TdsqlHostInfo> monitoredTdsqlHostInfoSet;
    /**
     * 保存每个DataSource的第一次心跳检测计数器，它需要多个线程间可见
     */
    private volatile Map<String, CountDownLatch> firstCheckFinishedMap;

    private TdsqlLoadBalanceHeartbeatMonitor() {
    }

    public void initialize(TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo) {
        // 仅初始化一次，建立核心线程数为10的调度任务线程池，之后的心跳检测任务都会提交到这个线程池去执行
        // 核心线程数设置为10是考虑到，如果存在多个数据源，并且每个数据源都使用负载均衡建立连接的情况
        // 如果只有一个数据源，这种情况比较普遍，大概率配置的IP地址个数达不到核心线程数
        if (!this.heartbeatMonitorInitialized) {
            TdsqlLoggerFactory.logInfo("Heartbeat monitor initializing.");
            this.heartbeatMonitor = new ScheduledThreadPoolExecutor(10,
                    new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Heartbeat-pool-%d").build());
            this.monitoredTdsqlHostInfoSet = new HashSet<>();
            this.firstCheckFinishedMap = new HashMap<>();
            this.heartbeatMonitorInitialized = true;
            TdsqlLoggerFactory.logInfo("Heartbeat monitor initialized.");
        }

        // 根据生成的DataSourceUuid，初始化第一次心跳检测完成计数器
        // 计数器的大小设置为该DataSource里面配置的IP地址的个数，每个IP地址心跳检测完成后，计数器递减
        String datasourceUuid = tdsqlLoadBalanceInfo.getDatasourceUuid();
        if (!this.firstCheckFinishedMap.containsKey(datasourceUuid)) {
            this.firstCheckFinishedMap.put(datasourceUuid,
                    new CountDownLatch(tdsqlLoadBalanceInfo.getTdsqlHostInfoList().size()));
            TdsqlLoggerFactory.logInfo("Found new datasource [" + datasourceUuid + "]");
        }

        // 判断IP地址列表中的IP地址是否已经加入过心跳检测任务，避免相同的IP地址重复加入
        for (TdsqlHostInfo tdsqlHostInfo : tdsqlLoadBalanceInfo.getTdsqlHostInfoList()) {
            if (this.monitoredTdsqlHostInfoSet.contains(tdsqlHostInfo)) {
                continue;
            }
            this.heartbeatMonitor.scheduleWithFixedDelay(new HeartbeatMonitorTask(tdsqlHostInfo,
                            tdsqlLoadBalanceInfo.getTdsqlLoadBalanceMaximumErrorRetries(), this.firstCheckFinishedMap), 0L,
                    tdsqlLoadBalanceInfo.getTdsqlLoadBalanceHeartbeatIntervalTime(), TimeUnit.MILLISECONDS);
            this.monitoredTdsqlHostInfoSet.add(tdsqlHostInfo);
            TdsqlLoggerFactory.logInfo("Add new host to heartbeat monitor. [ds: " + datasourceUuid + ", host:"
                    + tdsqlHostInfo.getHostPortPair() + "]");
        }
    }

    public CountDownLatch getFirstCheckFinished(String datasourceUuid) {
        return this.firstCheckFinishedMap.get(datasourceUuid);
    }

    /**
     * <p>心跳检测任务静态内部类</p>
     */
    private static class HeartbeatMonitorTask implements Runnable {

        /**
         * 需要进行心跳检测的IP地址信息
         */
        private final TdsqlHostInfo tdsqlHostInfo;
        /**
         * 心跳检测失败后的最大尝试次数
         */
        private final int retries;
        /**
         * 每个DataSource的第一次心跳检测计数器的引用
         */
        private final Map<String, CountDownLatch> firstCheckFinishedMap;
        /**
         * 标识线程是否是第一次执行该任务
         */
        private boolean isFirstCheck = true;

        public HeartbeatMonitorTask(TdsqlHostInfo tdsqlHostInfo, int retries,
                Map<String, CountDownLatch> firstCheckFinishedMap) {
            this.tdsqlHostInfo = tdsqlHostInfo;
            this.retries = retries;
            this.firstCheckFinishedMap = firstCheckFinishedMap;
        }

        @Override
        public void run() {
            try {
                TdsqlLoggerFactory.logInfo("Start heartbeat monitor check [" + tdsqlHostInfo.getHostPortPair() + "]");
                int attemptCount = 0;

                // 设置建立心跳检测连接的超时时间为1秒，同时需要保留改IP地址设置的其它参数设置
                Properties properties = tdsqlHostInfo.exposeAsProperties();
                properties.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
                properties.setProperty(PropertyKey.connectTimeout.getKeyName(), "1000");
                Map<String, String> map = new HashMap<>(properties.size());
                for (Entry<Object, Object> entry : properties.entrySet()) {
                    map.put((String) entry.getKey(), (String) entry.getValue());
                }
                HostInfo heartbeatHostInfo = new HostInfo(tdsqlHostInfo.getOriginalUrl(), tdsqlHostInfo.getHost(),
                        tdsqlHostInfo.getPort(), tdsqlHostInfo.getUser(), tdsqlHostInfo.getPassword(), map);

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
                        TdsqlLoadBalanceBlacklistHolder.getInstance().removeBlacklist(tdsqlHostInfo);

                        // 如果是第一次执行该逻辑，更新执行标识
                        // 并根据该IP地址所属的DataSourceUuid，获取到第一次心跳检测计数器，对其进行更新
                        if (this.isFirstCheck) {
                            this.isFirstCheck = false;
                            this.firstCheckFinishedMap.get(tdsqlHostInfo.getOwnerUuid()).countDown();
                        }
                        // 心跳检测成功记录调试级别日志，退出当前循环后，等待下次调度
                        TdsqlLoggerFactory.logInfo(
                                "Success heartbeat monitor check [" + tdsqlHostInfo.getHostPortPair() + "]");
                        break;
                    } catch (SQLException e) {
                        // 计算并比较心跳检测次数是否达到允许的最大次数
                        // 如果没有达到，则马上继续下次心跳检测
                        // 否则，将该IP地址加入黑名单并记录错误级别的日志，同时更新首次检测标识和计数器
                        if (attemptCount + 1 > retries) {
                            // 加入黑名单
                            TdsqlLoggerFactory.logError("Host heartbeat monitor failed now attempts [" + attemptCount
                                    + "] equals max attempts [" + retries + "], try add to blacklist. HostInfo ["
                                    + tdsqlHostInfo.getHostPortPair() + "]", e);
                            TdsqlLoadBalanceBlacklistHolder.getInstance().addBlacklist(tdsqlHostInfo);

                            // 如果是第一次执行该逻辑，更新执行标识
                            // 并根据该IP地址所属的DataSourceUuid，获取到第一次心跳检测计数器，对其进行更新
                            if (this.isFirstCheck) {
                                this.isFirstCheck = false;
                                this.firstCheckFinishedMap.get(tdsqlHostInfo.getOwnerUuid()).countDown();
                            }
                        } else {
                            // 心跳检测失败处理逻辑，程序执行到这里有可能是建立连接失败、超时，或执行心跳检测SQL失败、超时。
                            // 无论是上述哪种情况，都需要记录错误级别日志
                            TdsqlLoggerFactory.logError(
                                    "Host heartbeat monitor failed and try again, now attempts [" + attemptCount
                                            + "], max attempts [" + retries + "]. HostInfo ["
                                            + tdsqlHostInfo.getHostPortPair() + "]", e);
                        }
                        ++attemptCount;
                    }
                }
            } catch (Exception e) {
                TdsqlLoggerFactory.logError(
                        "Host heartbeat monitor check error [" + tdsqlHostInfo.getHostPortPair() + "]", e);
            }
        }
    }

    public static TdsqlLoadBalanceHeartbeatMonitor getInstance() {
        return SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlLoadBalanceHeartbeatMonitor INSTANCE = new TdsqlLoadBalanceHeartbeatMonitor();
    }
}
