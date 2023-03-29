package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode.LOAD_BALANCE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logDebug;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logFatal;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_ERROR_RETRY_INTERVAL_TIME_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_INTERVAL_TIME_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MAX_ERROR_RETRIES;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_FALSE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_TRUE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.TDSQL_LOAD_BALANCE_STRATEGY_LC;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConst.TDSQL_LOAD_BALANCE_STRATEGY_SED;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlBalanceStrategyFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>TDSQL-NySQL专属的，在建立负载均衡的数据库连接时，实现了连接收敛的处理类</p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlLoadBalanceConnectionFactory {

    /**
     * 标识是否启用TDSQL-MySQL专属的负载均衡模式
     * 目的是在关闭连接时，有针对性的进行额外的处理，
     * 这里额外的处理主要的目的是：关闭连接的同时，对全局连接计数器进行更新
     */
    public static boolean tdsqlLoadBalanceMode = false;

    private TdsqlLoadBalanceConnectionFactory() {
    }

    /**
     * <p>获取一个连接的前置处理逻辑，对特有的URL参数进行校验和赋值</p>
     *
     * @param connectionUrl {@link ConnectionUrl}
     * @return 返回 {@link JdbcConnection} 接口的一个实例，这里就是 {@link ConnectionImpl} 对象的实例
     * @throws SQLException 当有异常时抛出
     */
    public JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
        // 设置专属负载均衡模式标识
        tdsqlLoadBalanceMode = true;
        logDebug("Receive one of create load balance request. [" + connectionUrl + "]");
        Properties props = connectionUrl.getConnectionArgumentsAsProperties();

        List<HostInfo> hostsList = connectionUrl.getHostsList();
        int numHosts = hostsList.size();

        // 转化为TDSQL自己的HostInfo对象，该对象继承自HostInfo
        List<TdsqlHostInfo> tdsqlHostInfoList = new ArrayList<>(numHosts);
        for (HostInfo hostInfo : hostsList) {
            tdsqlHostInfoList.add(new TdsqlHostInfo(hostInfo, LOAD_BALANCE));
        }

        // 解析并校验连接参数
        TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo = this.validateConnectionAttributes(props, tdsqlHostInfoList,
                numHosts, connectionUrl);

        // 进入获取连接核心处理逻辑
        return createNewConnection(tdsqlLoadBalanceInfo);
    }

    /**
     * <p>获取连接核心处理逻辑</p>
     *
     * @param tdsqlLoadBalanceInfo {@link TdsqlLoadBalanceInfo}
     * @return 返回 {@link JdbcConnection} 接口的一个实例，这里就是 {@link ConnectionImpl} 对象的实例
     * @throws SQLException 当有异常时抛出
     */
    private synchronized JdbcConnection createNewConnection(TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo)
            throws SQLException {
        // 初始化全局连接计数器
        TdsqlLoadBalanceConnectionCounter.getInstance().initialize(tdsqlLoadBalanceInfo);

        // 当开启心跳检测开关时，初始化心跳检测监视器
        if (tdsqlLoadBalanceInfo.isTdsqlLoadBalanceHeartbeatMonitorEnable()) {
            TdsqlLoadBalanceHeartbeatMonitor.getInstance().initialize(tdsqlLoadBalanceInfo);

            // 等待心跳检测监视器初始化完成，并完成第一次所有IP地址的心跳检测
            // 每一个DataSource都会持有自己专属的计数器，且当该DataSource第一次建立连接时，计数器非零，后面的等待超时逻辑才会生效
            // 对于每一个DataSource，该逻辑只会生效一次
            // 这时，如果IP地址无法建立数据库连接，则该IP地址会被加入黑名单
            // 同时，该IP地址会在全局连接计数器中被移除，被移除的IP地址在之后的负载均衡算法策略中不会被调度
            List<CountDownLatch> latchList = TdsqlLoadBalanceHeartbeatMonitor.getInstance()
                    .getFirstCheckFinished(tdsqlLoadBalanceInfo.getIpPortSet());
            for (CountDownLatch latch : latchList) {
                if (latch.getCount() != 0L) {
                    try {
                        // 考虑到有可能在第一次心跳检测时，存在建立数据库连接无法及时响应返回的情况（表象是建立连接卡住）
                        // 在这里设置了等待检测结果的超时时间，设置为了需要检测的IP地址个数乘以重试次数加一次再乘以2秒
                        // 之所以乘以2秒，是因为心跳检测建立连接的超时时间为1秒，之后执行检测SQL语句的超时时间也为1秒
                        // 因为多个IP地址的检测时并行进行的，因此等待超时设置为该值也就变得足够了
                        boolean await = latch.await(tdsqlLoadBalanceInfo.getTdsqlHostInfoList().size() * (
                                        tdsqlLoadBalanceInfo.getTdsqlLoadBalanceHeartbeatMaxErrorRetries() + 1) * 2L,
                                TimeUnit.SECONDS);
                        // 如果等待第一次心跳检测结果超时了，说明应用程序在第一次启动时，网络环境或者后端数据库存在异常
                        // 此时，我们会记录错误级别的日志，同时抛出异常阻止应用程序建立连接
                        if (!await) {
                            String errMessage = "Wait for first heartbeat check finished timeout!";
                            logError(errMessage);
                            throw SQLError.createSQLException(errMessage,
                                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, null);
                        }
                    } catch (InterruptedException e) {
                        String errMessage = "Wait for first heartbeat check finished timeout!";
                        logError(errMessage, e);
                        throw SQLError.createSQLException(errMessage,
                                MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, null);
                    }
                }
            }
            logInfo("All host in current datasource has finished first heartbeat checked!");
            if (TdsqlLoadBalanceBlacklistHolder.getInstance().isBlacklistEnabled()) {
                logInfo("Current blacklist [" + TdsqlLoadBalanceBlacklistHolder.getInstance()
                        .printBlacklist() + "]");
            }
        }

        // 根据全局连接计数器，执行负载均衡算法策略，选择出一个需要建立数据库连接的IP地址
        TdsqlAtomicLongMap<TdsqlHostInfo> counter = TdsqlLoadBalanceConnectionCounter.getInstance()
                .getCounter(tdsqlLoadBalanceInfo.getDatasourceUuid());
        // 如果全局连接计数器是空的，则记录严重错误级别的日志，并提前抛出异常提醒用户
        if (counter == null || counter.isEmpty()) {
            String errMessage = "Could not create connection to database server.";
            logFatal(errMessage);
            logFatal("Current blacklist [" + TdsqlLoadBalanceBlacklistHolder.getInstance()
                    .printBlacklist() + "]");
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                    null);
        }

        // 初始化负载均衡算法策略对象，目前支持SED算法策略，如果后续算法策略扩展，这里会做相应的修改
        TdsqlLoadBalanceStrategy strategy = TdsqlBalanceStrategyFactory.getInstance()
                .getStrategyInstance(tdsqlLoadBalanceInfo.getTdsqlLoadBalanceStrategy());
        TdsqlHostInfo choice = strategy.choice(counter);
        // 如果负载均衡算法策略无法选择出IP地址，大概率是因为全局连接计数器是空的
        // 也就是说，所有的IP地址都被加入了黑名单，无法再进行调度
        // 这种情况出现的概率较小，我们会记录严重错误级别的日志，并提前抛出异常提醒用户
        if (choice == null) {
            String errMessage = "Could not create connection to database server. "
                    + "LoadBalanced Strategy not choice any hosts.";
            logFatal(errMessage);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                    null);
        }
        try {
            // 使用负载均衡算法策略选择的IP地址建立物理数据库连接
            // 同时操作全局连接计数器对其进行计数
            JdbcConnection connection = ConnectionImpl.getInstance(choice);
            TdsqlLoadBalanceConnectionCounter.getInstance().incrementCounter(choice);
            logInfo("Create connection success [" + choice.getHostPortPair() + "], return it.");
            return connection;
        } catch (SQLException e) {
            // 如果建立数据库连接失败，记录日志和堆栈、抛出异常
            if (tdsqlLoadBalanceInfo.isTdsqlLoadBalanceHeartbeatMonitorEnable()) {
                // 如果开启了心跳检测，则黑名单也就开启了，将该失败的IP地址加入黑名单
                // 保证这个IP地址在心跳检测成功之前，不再被调度到
                logError("Could not create connection to database server [" + choice.getHostPortPair()
                        + "], try add to blacklist.", e);
                TdsqlLoadBalanceBlacklistHolder.getInstance().addBlacklist(choice);
            } else {
                // 同时将重置该失败的IP地址的连接计数器
                logError("Could not create connection to database server [" + choice.getHostPortPair()
                        + "], remove its counter.", e);
                TdsqlLoadBalanceConnectionCounter.getInstance().resetCounter(choice);
            }
            throw e;
        }
    }

    public static TdsqlLoadBalanceConnectionFactory getInstance() {
        return TdsqlLoadBalanceConnectionFactory.SingletonInstance.INSTANCE;
    }

    /**
     * <p>解析并校验连接参数</p>
     *
     * @param props 待解析并校验的参数
     * @param tdsqlHostInfoList 主机列表
     * @param numHosts 主机个数
     * @return {@link TdsqlLoadBalanceInfo}
     * @throws SQLException 当连接参数解析或校验失败时
     */
    private TdsqlLoadBalanceInfo validateConnectionAttributes(Properties props, List<TdsqlHostInfo> tdsqlHostInfoList,
            int numHosts, ConnectionUrl connectionUrl) throws SQLException {
        // 初始化TDSQL负载均衡信息记录类对象，并在依次解析URL参数后对其赋值
        // 每个负载均衡信息记录类都有自己的DataSourceUuid
        // 该DataSourceUuid是由 TdsqlLoadBalanceInfo 的 setTdsqlHostInfoList 方法生成并赋值的
        TdsqlLoadBalanceInfo tdsqlLoadBalanceInfo = new TdsqlLoadBalanceInfo();
        tdsqlLoadBalanceInfo.setTdsqlHostInfoList(tdsqlHostInfoList, connectionUrl);

        // 解析并校验“策略算法”参数，目前允许设置为"SED"或者"LC"
        String tdsqlLoadBalanceStrategyStr = props.getProperty(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName(),
                TDSQL_LOAD_BALANCE_STRATEGY_SED);
        if (!TDSQL_LOAD_BALANCE_STRATEGY_SED.equalsIgnoreCase(tdsqlLoadBalanceStrategyStr)
                && !TDSQL_LOAD_BALANCE_STRATEGY_LC.equalsIgnoreCase(tdsqlLoadBalanceStrategyStr)) {
            String errMessage = Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceStrategy",
                    new Object[]{tdsqlLoadBalanceStrategyStr}) + Messages.getString(
                    "ConnectionProperties.tdsqlLoadBalanceStrategy");
            logError(errMessage);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    null);
        }
        tdsqlLoadBalanceInfo.setTdsqlLoadBalanceStrategy(tdsqlLoadBalanceStrategyStr);

        // 由于负载因子需要与IP地址一一对应，因此加入了一些必要的处理逻辑
        // 1.当负载因子少于IP地址的个数时，缺少的负载因子会被赋值为默认值1
        // 2.当负载因子多于IP地址的个数时，多于的负载因子将被抛弃
        List<Integer> tdsqlLoadBalanceWeightFactorList = new ArrayList<>(numHosts);
        for (int i = 0; i < numHosts; i++) {
            tdsqlLoadBalanceWeightFactorList.add(1);
        }
        String tdsqlLoadBalanceWeightFactorStr = props.getProperty(
                PropertyKey.tdsqlLoadBalanceWeightFactor.getKeyName(), null);
        if (!StringUtils.isNullOrEmpty(tdsqlLoadBalanceWeightFactorStr)) {
            List<String> factorArray = StringUtils.split(tdsqlLoadBalanceWeightFactorStr, ",", true);
            for (int i = 0; i < factorArray.size(); i++) {
                if (i >= numHosts) {
                    break;
                }
                try {
                    int wf = Integer.parseInt(factorArray.get(i));
                    if (wf < 0) {
                        String errMessage =
                                Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceWeightFactor",
                                        new Object[]{factorArray.get(i)}) + Messages.getString(
                                        "ConnectionProperties.tdsqlLoadBalanceWeightFactor");
                        logError(errMessage);
                        throw SQLError.createSQLException(errMessage,
                                MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
                    }
                    tdsqlLoadBalanceWeightFactorList.set(i, wf);
                } catch (NumberFormatException e) {
                    String errMessage =
                            Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceWeightFactor",
                                    new Object[]{factorArray.get(i)}) + Messages.getString(
                                    "ConnectionProperties.tdsqlLoadBalanceWeightFactor");
                    logError(errMessage, e);
                    throw SQLError.createSQLException(errMessage,
                            MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
                }
            }
        }
        tdsqlLoadBalanceInfo.setTdsqlLoadBalanceWeightFactorList(tdsqlLoadBalanceWeightFactorList);
        for (int i = 0; i < numHosts; i++) {
            tdsqlHostInfoList.get(i).setWeightFactor(tdsqlLoadBalanceWeightFactorList.get(i));
        }

        // 解析并校验“心跳检测开关”参数，该参数默认值为ture，代表开启心跳检测
        String tdsqlLoadBalanceHeartbeatMonitorStr = props.getProperty(
                PropertyKey.tdsqlLoadBalanceHeartbeatMonitorEnable.getKeyName(),
                String.valueOf(DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE));
        try {
            if (!TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_TRUE.equalsIgnoreCase(tdsqlLoadBalanceHeartbeatMonitorStr)
                    && !TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_FALSE.equalsIgnoreCase(
                    tdsqlLoadBalanceHeartbeatMonitorStr)) {
                String errMessage =
                        Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatMonitorEnable",
                                new Object[]{tdsqlLoadBalanceHeartbeatMonitorStr}) + Messages.getString(
                                "ConnectionProperties.tdsqlLoadBalanceHeartbeatMonitorEnable");
                logError(errMessage);
                throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                        null);
            }
            boolean tdsqlLoadBalanceHeartbeatMonitor = Boolean.parseBoolean(tdsqlLoadBalanceHeartbeatMonitorStr);
            tdsqlLoadBalanceInfo.setTdsqlLoadBalanceHeartbeatMonitorEnable(tdsqlLoadBalanceHeartbeatMonitor);
            // 如果主动关闭了心跳检测，则不再启用黑名单
            if (!tdsqlLoadBalanceHeartbeatMonitor) {
                TdsqlLoadBalanceBlacklistHolder.getInstance().setBlacklistEnabled(false);
            }
        } catch (Exception e) {
            String errMessage =
                    Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatMonitorEnable",
                            new Object[]{tdsqlLoadBalanceHeartbeatMonitorStr}) + Messages.getString(
                            "ConnectionProperties.tdsqlLoadBalanceHeartbeatMonitorEnable");
            logError(errMessage, e);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    null);
        }

        // 解析并校验“心跳检测时间间隔”参数，该参数默认值为3000，单位为毫秒
        // 考虑到对性能的影响，该参数被允许设置的最小值为1000
        String tdsqlLoadBalanceHeartbeatIntervalTimeStr = props.getProperty(
                PropertyKey.tdsqlLoadBalanceHeartbeatIntervalTimeMillis.getKeyName(),
                String.valueOf(DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_INTERVAL_TIME_MILLIS));
        try {
            int tdsqlLoadBalanceHeartbeatIntervalTime = Integer.parseInt(tdsqlLoadBalanceHeartbeatIntervalTimeStr);
            if (tdsqlLoadBalanceHeartbeatIntervalTime < 1000) {
                String errMessage = Messages.getString(
                        "ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatIntervalTimeMillis",
                        new Object[]{tdsqlLoadBalanceHeartbeatIntervalTimeStr}) + Messages.getString(
                        "ConnectionProperties.tdsqlLoadBalanceHeartbeatIntervalTimeMillis");
                logError(errMessage);
                throw SQLError.createSQLException(errMessage,
                        MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
            }
            tdsqlLoadBalanceInfo.setTdsqlLoadBalanceHeartbeatIntervalTimeMillis(tdsqlLoadBalanceHeartbeatIntervalTime);
        } catch (NumberFormatException e) {
            String errMessage = Messages.getString(
                    "ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatIntervalTimeMillis",
                    new Object[]{tdsqlLoadBalanceHeartbeatIntervalTimeStr}) + Messages.getString(
                    "ConnectionProperties.tdsqlLoadBalanceHeartbeatIntervalTimeMillis");
            logError(errMessage, e);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    null);
        }

        // 解析并校验“心跳检测失败的最大尝试次数”参数，该参数默认值为1次
        String tdsqlLoadBalanceMaximumErrorRetriesStr = props.getProperty(
                PropertyKey.tdsqlLoadBalanceHeartbeatMaxErrorRetries.getKeyName(),
                String.valueOf(DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MAX_ERROR_RETRIES));
        try {
            int tdsqlLoadBalanceMaximumErrorRetries = Integer.parseInt(tdsqlLoadBalanceMaximumErrorRetriesStr);
            if (tdsqlLoadBalanceMaximumErrorRetries <= 0) {
                String errMessage =
                        Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatMaxErrorRetries",
                                new Object[]{tdsqlLoadBalanceMaximumErrorRetriesStr}) + Messages.getString(
                                "ConnectionProperties.tdsqlLoadBalanceHeartbeatMaxErrorRetries");
                logError(errMessage);
                throw SQLError.createSQLException(errMessage,
                        MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
            }
            tdsqlLoadBalanceInfo.setTdsqlLoadBalanceHeartbeatMaxErrorRetries(tdsqlLoadBalanceMaximumErrorRetries);
        } catch (NumberFormatException e) {
            String errMessage =
                    Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatMaxErrorRetries",
                            new Object[]{tdsqlLoadBalanceMaximumErrorRetriesStr}) + Messages.getString(
                            "ConnectionProperties.tdsqlLoadBalanceHeartbeatMaxErrorRetries");
            logError(errMessage, e);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    null);
        }

        // 解析并校验“心跳检测失败的重试间隔时间"参数，参数默认值为5000，单位为毫秒
        String tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillisStr = props.getProperty(
                PropertyKey.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis.getKeyName(),
                String.valueOf(DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_ERROR_RETRY_INTERVAL_TIME_MILLIS));
        try {
            int tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis = Integer.parseInt(
                    tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillisStr);
            if (tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis < 0 || (
                    tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis > 0
                            && tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis < 100)) {
                String errMessage = Messages.getString(
                        "ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis",
                        new Object[]{tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillisStr}) + Messages.getString(
                        "ConnectionProperties.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis");
                logError(errMessage);
                throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                        null);
            }
            tdsqlLoadBalanceInfo.setTdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis(
                    tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis);
        } catch (NumberFormatException e) {
            String errMessage = Messages.getString(
                    "ConnectionProperties.badValueForTdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis",
                    new Object[]{tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillisStr}) + Messages.getString(
                    "ConnectionProperties.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis");
            logError(errMessage, e);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    null);
        }

        return tdsqlLoadBalanceInfo;
    }

    public synchronized void closeConnection(TdsqlHostInfo tdsqlHostInfo) {
        if (Objects.equals(LOAD_BALANCE, tdsqlHostInfo.getConnectionMode())) {
            logInfo("LoadBalance Mode close method called. [" + tdsqlHostInfo.getHostPortPair() + "]");
            TdsqlLoadBalanceConnectionCounter.getInstance().decrementCounter(tdsqlHostInfo);
        }
    }

    private static class SingletonInstance {

        private static final TdsqlLoadBalanceConnectionFactory INSTANCE = new TdsqlLoadBalanceConnectionFactory();
    }
}
