package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl.Type.SINGLE_CONNECTION;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_CONNECTION_TIME_ZONE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_CONNECT_TIMEOUT_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_SOCKET_TIMEOUT_MILLIS;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.DOUBLE_SLASH_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.SLASH_MARK;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectRefreshTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlSynchronousExecutor;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>TDSQL专属，直连模式拓扑刷新服务类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyServer {

    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final List<HostInfo> proxyHostInfoList;
    private final Map<String, TdsqlDirectProxyConnectionHolder> liveProxyConnectionMap;
    private final Map<String, Long> proxyBlacklist;
    private final ScheduledThreadPoolExecutor topologyRefreshExecutor;
    private final Executor netTimeoutExecutor;

    public TdsqlDirectRefreshTopologyTask getRefreshTopologyTask() {
        return refreshTopologyTask;
    }

    private TdsqlDirectRefreshTopologyTask refreshTopologyTask;

    /**
     * 构造方法
     *
     * @param dataSourceConfig 数据源配置信息
     */
    public TdsqlDirectTopologyServer(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
        this.proxyHostInfoList = this.rebuildHostInfoForProxy(dataSourceConfig.getTdsqlDirectProxyHostInfoList());
        this.liveProxyConnectionMap = new HashMap<>(this.proxyHostInfoList.size());
        this.proxyBlacklist = new HashMap<>();
        // 初始化刷新拓扑信息计划任务线程池
        this.topologyRefreshExecutor = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("TopologyRefresh-" + dataSourceConfig.getDataSourceUuid().substring(24, 32))
                        .build());
        // 初始化Proxy连接SocketTimeout执行器
        this.netTimeoutExecutor = new TdsqlSynchronousExecutor(dataSourceConfig.getDataSourceUuid());
    }

    /**
     * 开始刷新拓扑信息
     */
    public void startRefreshTopology() {
        List<String> unmodifiableHostPortList = Collections.unmodifiableList(
                this.proxyHostInfoList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList()));

        Map<String, TdsqlDirectProxyConnectionHolder> unmodifiableLiveConnectionMap = Collections.unmodifiableMap(
                this.liveProxyConnectionMap);
        this.refreshTopologyTask = new TdsqlDirectRefreshTopologyTask(this.dataSourceConfig, unmodifiableHostPortList,
                unmodifiableLiveConnectionMap);
        // 根据配置的拓扑信息刷新间隔阈值，开始执行计划任务
        this.topologyRefreshExecutor.scheduleWithFixedDelay(
                refreshTopologyTask, 0L,
                this.dataSourceConfig.getTdsqlDirectTopoRefreshIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 根据时间戳，判断是否需要重建与Proxy的数据库连接
     *
     * @param holdTimeMillis 时间戳
     * @return 如果需要重建返回 {@code true}，否则返回 {@code false}
     */
    public boolean needReconnectProxy(long holdTimeMillis) {
        return System.currentTimeMillis() - holdTimeMillis
                >= this.dataSourceConfig.getTdsqlDirectReconnectProxyIntervalTimeSeconds() * 1000;
    }

    /**
     * 判断已建立的Proxy数据库连接是否有效
     *
     * @param jdbcConnection 已建立的Proxy数据库连接
     * @return 如果有效返回 {@code true}，否则返回 {@code false}
     */
    public boolean isProxyConnectionValid(JdbcConnection jdbcConnection) {
        try {
            if (!jdbcConnection.isValid(3)) {
                TdsqlLoggerFactory.logInfo(
                        Messages.getString("TdsqlDirectRefreshTopologyMessage.InvalidProxyConnection",
                                new Object[]{jdbcConnection.getHostPortPair()}));
                this.closeProxyConnection(jdbcConnection.getHostPortPair());
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                    Messages.getString("TdsqlDirectRefreshTopologyException.FailedToValidProxyConnection"));
        }
    }

    /**
     * 根据Proxy的主机地址和端口号字符串，关闭其已建立的连接
     *
     * @param hostPortPair 主机地址和端口号字符串
     */
    public void closeProxyConnection(String hostPortPair) {
        try {
            TdsqlDirectProxyConnectionHolder connectionHolder = this.liveProxyConnectionMap.get(hostPortPair);
            if (connectionHolder != null) {
                // 关闭连接
                connectionHolder.getJdbcConnection().close();
                // 移除缓存映射
                this.liveProxyConnectionMap.remove(hostPortPair);
                // 从Proxy黑名单中移除
                this.proxyBlacklist.remove(hostPortPair);
            }
        } catch (Throwable t) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                    Messages.getString("TdsqlDirectRefreshTopologyException.FailedToCloseExpiredProxyConnection"));
        }
    }

    /**
     * 获取Proxy黑名单
     *
     * @return 主机地址和端口字符串与黑名单超时时间的映射
     */
    public Map<String, Long> getProxyBlacklist() {
        // 已加入黑名单的主机地址和端口字符串集合
        Set<String> keys = this.proxyBlacklist.keySet();

        // 整理黑名单，移除黑名单中存在，但在可用主机列表中不存在的主机信息
        this.proxyHostInfoList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList())
                .forEach(keys::remove);

        // 如果达到了黑名单超时阈值，则从黑名单中移除
        for (Iterator<String> it = keys.iterator(); it.hasNext(); ) {
            String hostPortPair = it.next();
            Long timeout = this.proxyBlacklist.get(hostPortPair);
            if (timeout != null && timeout < System.currentTimeMillis()) {
                this.proxyBlacklist.remove(hostPortPair);
                it.remove();
            }
        }

        // 如果所有的主机都不可用，返回一个空的黑名单，因为我们不想等待 tdsqlDirectProxyBlacklistTimeoutSeconds 过期
        if (keys.size() == this.proxyHostInfoList.size()) {
            return new HashMap<>();
        }
        return this.proxyBlacklist;
    }

    /**
     * 根据主机地址和端口字符串加入黑名单
     *
     * @param hostPortPair 主机地址和端口字符串
     */
    public void addToBlacklist(String hostPortPair) {
        this.proxyBlacklist.put(hostPortPair,
                System.currentTimeMillis() + this.dataSourceConfig.getTdsqlDirectProxyBlacklistTimeoutSeconds() * 1000);
    }

    /**
     * 根据主机地址和端口字符串，创建数据库连接
     *
     * @param hostPortPair 主机地址和端口字符串
     * @return Proxy连接包装类
     * @throws SQLException 建立数据库连接失败时抛出
     */
    public TdsqlDirectProxyConnectionHolder createConnectionForHost(String hostPortPair) throws SQLException {
        for (HostInfo hi : this.proxyHostInfoList) {
            if (hi.getHostPortPair().equals(hostPortPair)) {
                // 建立连接
                ConnectionImpl conn = (ConnectionImpl) ConnectionImpl.getInstance(hi);
                // 设置连接的SocketTimeout
                conn.setNetworkTimeout(this.netTimeoutExecutor,
                        this.dataSourceConfig.getTdsqlDirectTopoRefreshConnTimeoutMillis());
                // 创建Proxy连接包装类
                TdsqlDirectProxyConnectionHolder connectionHolder = new TdsqlDirectProxyConnectionHolder(conn,
                        System.currentTimeMillis());
                // 保存已建立的连接
                this.liveProxyConnectionMap.put(hostPortPair, connectionHolder);
                // 并把其地址从黑名单中移除
                this.proxyBlacklist.remove(hostPortPair);
                return connectionHolder;
            }
        }
        return null;
    }

    /**
     * 为Proxy重建主机信息
     *
     * @param hostInfoList 待重建的主机信息列表
     * @return 重建后的主机信息列表
     */
    private List<HostInfo> rebuildHostInfoForProxy(List<HostInfo> hostInfoList) {
        List<HostInfo> proxyHostInfoList = new ArrayList<>(hostInfoList.size());
        for (HostInfo hi : hostInfoList) {
            String proxyUrl = SINGLE_CONNECTION.getScheme() + DOUBLE_SLASH_MARK + hi.getHostPortPair() + SLASH_MARK
                    + hi.getDatabase();
            Properties prop = new Properties();
            prop.setProperty(PropertyKey.USER.getKeyName(), hi.getUser());
            prop.setProperty(PropertyKey.PASSWORD.getKeyName(), hi.getPassword());
            prop.setProperty(PropertyKey.connectTimeout.getKeyName(),
                    String.valueOf(DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_CONNECT_TIMEOUT_MILLIS));
            prop.setProperty(PropertyKey.socketTimeout.getKeyName(),
                    String.valueOf(DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_SOCKET_TIMEOUT_MILLIS));
            prop.setProperty(PropertyKey.useSSL.getKeyName(), Boolean.FALSE.toString());
            prop.setProperty(PropertyKey.characterEncoding.getKeyName(), StandardCharsets.UTF_8.name());
            prop.setProperty(PropertyKey.connectionTimeZone.getKeyName(), DEFAULT_CONNECTION_TIME_ZONE);
            HostInfo proxyHostInfo = ConnectionUrl.getConnectionUrlInstance(proxyUrl, prop).getMainHost();
            proxyHostInfoList.add(proxyHostInfo);
        }
        return proxyHostInfoList;
    }

    public TdsqlDirectDataSourceConfig getDataSourceConfig() {
        return dataSourceConfig;
    }
}
