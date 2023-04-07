package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlInvalidConnectionPropertyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.IsValidRwModeReturned;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyEnum.IsAllowedStrategyReturned;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * <p>TDSQL专属，直连模式数据源配置信息类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectDataSourceConfig implements Serializable {

    private static final long serialVersionUID = 8448206855430138752L;

    private final String dataSourceUuid;
    private TdsqlDirectReadWriteModeEnum tdsqlDirectReadWriteMode;
    private TdsqlLoadBalanceStrategyEnum tdsqlLoadBalanceStrategy;
    private Integer tdsqlDirectTopoRefreshIntervalMillis;
    private Integer tdsqlDirectMaxSlaveDelaySeconds;
    private Boolean tdsqlDirectMasterCarryOptOfReadOnlyMode;
    private Integer tdsqlDirectTopoRefreshConnTimeoutMillis;
    private Integer tdsqlDirectTopoRefreshStmtTimeoutSeconds;
    private Integer tdsqlDirectCloseConnTimeoutMillis;
    private Integer tdsqlDirectProxyBlacklistTimeoutSeconds;
    private Integer tdsqlDirectReconnectProxyIntervalTimeSeconds;

    public Integer getTdsqlConnectionTimeOut() {
        return tdsqlConnectionTimeOut;
    }

    private Integer tdsqlConnectionTimeOut;
    private List<HostInfo> tdsqlDirectProxyHostInfoList;
    private Properties tdsqlDirectOriginalPropertiesWithoutDirectMode;
    private ConnectionUrl connectionUrl;
    private TdsqlDirectTopologyServer topologyServer;
    private TdsqlDirectCacheServer cacheServer;
    private TdsqlDirectScheduleServer scheduleServer;
    private TdsqlDirectConnectionManager connectionManager;
    private TdsqlDirectFailoverHandler failoverHandler;
    /**
     * URL信息校验并赋值
     *
     * @param connectionUrl {@link ConnectionUrl}
     */
    public void validateConnectionProperties(ConnectionUrl connectionUrl) {
        // 根据URL配置，初始化JdbcPropertySetImpl对象
        JdbcPropertySetImpl jdbcPropertySet = new JdbcPropertySetImpl();
        jdbcPropertySet.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

        // 1.开始判断读写分离模式
        String tdsqlDirectReadWriteMode = jdbcPropertySet.getStringProperty(PropertyKey.tdsqlDirectReadWriteMode)
                .getStringValue();
        IsValidRwModeReturned rwModeReturned = TdsqlDirectReadWriteModeEnum.isValidRwMode(tdsqlDirectReadWriteMode);
        if (!rwModeReturned.isValid()) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    rwModeReturned.getErrorMessage());
        }
        TdsqlDirectReadWriteModeEnum rwModeEnum = rwModeReturned.getRwModeEnum();

        // 2.读写分离模式有效，则赋值
        this.setTdsqlDirectReadWriteMode(rwModeEnum);

        // 3.只读模式下，开始判断负载均衡策略算法、从库延迟、主库承担只读
        if (TdsqlDirectReadWriteModeEnum.RO.equals(rwModeEnum)) {
            // 3.1 判断该负载均衡策略算法在直连模式下是否被允许使用
            String tdsqlLoadBalanceStrategy = jdbcPropertySet.getStringProperty(PropertyKey.tdsqlLoadBalanceStrategy)
                    .getStringValue();
            IsAllowedStrategyReturned strategyReturned = TdsqlLoadBalanceStrategyEnum.isAllowedStrategy(
                    TdsqlConnectionModeEnum.DIRECT, tdsqlLoadBalanceStrategy);
            if (!strategyReturned.isAllowed()) {
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class, strategyReturned.getErrorMessage());
            }

            // 3.2 判断该负载均衡策略算法在直连模式的某种读写分离模式下是否被允许使用
            strategyReturned = TdsqlLoadBalanceStrategyEnum.isAllowedStrategy(rwModeEnum, tdsqlLoadBalanceStrategy);
            if (!strategyReturned.isAllowed()) {
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class, strategyReturned.getErrorMessage());
            }

            // 3.3 负载均衡算法有效，则赋值，默认值rw
            this.setTdsqlLoadBalanceStrategy(strategyReturned.getStrategyEnum());

            // 3.4 判断从库延迟时间多大时会被踢除，可行范围是[0, 100000]，单位秒，默认值0
            // 最大值 100000 代表当主从复制断开后，Proxy返回的从库延迟的值
            Integer tdsqlDirectMaxSlaveDelaySeconds = jdbcPropertySet.getIntegerProperty(
                    PropertyKey.tdsqlDirectMaxSlaveDelaySeconds).getValue();
            if (tdsqlDirectMaxSlaveDelaySeconds < 0
                    || tdsqlDirectMaxSlaveDelaySeconds
                    > TdsqlDirectConst.MAXIMUM_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS) {
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectMaxSlaveDelaySeconds",
                                new Object[]{tdsqlDirectMaxSlaveDelaySeconds}));
            }

            // 3.5 从库最大延迟有效，则赋值
            this.setTdsqlDirectMaxSlaveDelaySeconds(tdsqlDirectMaxSlaveDelaySeconds);

            // 3.6 赋值主库承担只读
            this.setTdsqlDirectMasterCarryOptOfReadOnlyMode(
                    jdbcPropertySet.getBooleanProperty(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode).getValue());
        }

        // 4.判断拓扑刷新间隔时间，有效范围是[1000, 2147483647]，单位毫秒，默认值1000
        Integer tdsqlDirectTopoRefreshIntervalMillis = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectTopoRefreshIntervalMillis).getValue();
        if (tdsqlDirectTopoRefreshIntervalMillis < TdsqlDirectConst.DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_INTERVAL_MILLIS) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectTopoRefreshIntervalMillis",
                            new Object[]{tdsqlDirectTopoRefreshIntervalMillis}));
        }

        // 5.拓扑刷新间隔时间有效，则赋值
        this.setTdsqlDirectTopoRefreshIntervalMillis(tdsqlDirectTopoRefreshIntervalMillis);

        // 6.判断创建拓扑刷新连接超时时间是否有效，有效范围是[0, 2147483647]，单位毫秒，默认值1000
        Integer tdsqlDirectTopoRefreshConnTimeoutMillis = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectTopoRefreshConnTimeoutMillis).getValue();
        if (tdsqlDirectTopoRefreshConnTimeoutMillis < 0) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectTopoRefreshConnTimeoutMillis",
                            new Object[]{tdsqlDirectTopoRefreshConnTimeoutMillis}));
        }

        // 7.创建拓扑刷新连接超时时间有效，则赋值
        this.setTdsqlDirectTopoRefreshConnTimeoutMillis(tdsqlDirectTopoRefreshConnTimeoutMillis);

        // 8.判断关闭连接超时时间是否有效，有效范围是[0, 2147483647]，单位毫秒，默认值1000
        // 该参数在当前版本仅供内部调试使用
        Integer tdsqlDirectCloseConnTimeoutMillis = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectCloseConnTimeoutMillis).getValue();
        if (tdsqlDirectCloseConnTimeoutMillis < 0) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectCloseConnTimeoutMillis",
                            new Object[]{tdsqlDirectCloseConnTimeoutMillis}));
        }

        // 9.关闭连接超时时间有效，则赋值
        this.setTdsqlDirectCloseConnTimeoutMillis(tdsqlDirectCloseConnTimeoutMillis);

        // 10.判断拓扑刷新SQL执行超时时间是否有效，有效范围是[0, 2147483647]，单位秒，默认值1
        Integer tdsqlDirectTopoRefreshStmtTimeoutSeconds = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectTopoRefreshStmtTimeoutSeconds).getValue();
        if (tdsqlDirectTopoRefreshStmtTimeoutSeconds < 0) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectTopoRefreshStmtTimeoutSeconds",
                            new Object[]{tdsqlDirectTopoRefreshStmtTimeoutSeconds}));
        }

        // 11.拓扑刷新SQL执行超时时间有效，则赋值
        this.setTdsqlDirectTopoRefreshStmtTimeoutSeconds(tdsqlDirectTopoRefreshStmtTimeoutSeconds);

        // 12.判断建立Proxy连接失败被加入黑名单后的超时时间是否有效，有效范围是[0, 2147483647]，单位秒，默认值60
        Integer tdsqlDirectProxyBlacklistTimeoutSeconds = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectProxyBlacklistTimeoutSeconds).getValue();
        if (tdsqlDirectProxyBlacklistTimeoutSeconds < 0) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectProxyBlacklistTimeoutSeconds",
                            new Object[]{tdsqlDirectProxyBlacklistTimeoutSeconds}));
        }

        // 13.建立Proxy连接失败被加入黑名单后的超时时间有效，则赋值
        this.setTdsqlDirectProxyBlacklistTimeoutSeconds(tdsqlDirectProxyBlacklistTimeoutSeconds);

        // 14.判断重建Proxy连接的间隔时间是否有效，有效范围是[30, 3600]，单位秒，默认值600
        Integer tdsqlDirectReconnectProxyIntervalTimeSeconds = jdbcPropertySet.getIntegerProperty(
                PropertyKey.tdsqlDirectReconnectProxyIntervalTimeSeconds).getValue();
        if (tdsqlDirectReconnectProxyIntervalTimeSeconds
                < TdsqlDirectConst.MINIMUM_TDSQL_DIRECT_RECONNECT_PROXY_INTERVAL_TIME_SECONDS
                || tdsqlDirectReconnectProxyIntervalTimeSeconds
                > TdsqlDirectConst.MAXIMUM_TDSQL_DIRECT_RECONNECT_PROXY_INTERVAL_TIME_SECONDS) {
            throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlInvalidConnectionPropertyException.class,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectReconnectProxyIntervalTimeSeconds",
                            new Object[]{tdsqlDirectReconnectProxyIntervalTimeSeconds}));
        }

        // 15.重建Proxy连接的间隔时间有效，则赋值
        this.setTdsqlDirectReconnectProxyIntervalTimeSeconds(
                tdsqlDirectReconnectProxyIntervalTimeSeconds);

        // 16.赋值所有配置的Proxy地址
        this.setTdsqlDirectProxyHostInfoList(connectionUrl.getHostsList());

        Integer connectionTimeout = jdbcPropertySet.getIntegerProperty(PropertyKey.connectTimeout).getValue();
        if (connectionTimeout == 0) {
            connectionTimeout = 1000 * 60;
        }
        this.tdsqlConnectionTimeOut = connectionTimeout;

        // 17.赋值URL原始参数信息，其中去除了直连模式特有的参数
        this.setTdsqlDirectOriginalPropertiesWithoutDirectMode(
                removeAllDirectModeProperties(connectionUrl.getMainHost().exposeAsProperties()));

        // 18.赋值ConnectionUrl
        this.setConnectionUrl(connectionUrl);
    }

    /**
     * 从原始配置中删除直连模式特有配置
     *
     * @param originalProperties 原始配置信息
     * @return 删除直连模式特有配置后的配置信息
     */
    public static Properties removeAllDirectModeProperties(Properties originalProperties) {
        Properties props = new Properties();
        props.putAll(originalProperties);
        props.remove(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName());
        props.remove(PropertyKey.tdsqlDirectReadWriteMode.getKeyName());
        props.remove(PropertyKey.tdsqlDirectTopoRefreshIntervalMillis.getKeyName());
        props.remove(PropertyKey.tdsqlDirectMaxSlaveDelaySeconds.getKeyName());
        props.remove(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName());
        props.remove(PropertyKey.tdsqlDirectTopoRefreshConnTimeoutMillis.getKeyName());
        props.remove(PropertyKey.tdsqlDirectCloseConnTimeoutMillis.getKeyName());
        props.remove(PropertyKey.tdsqlDirectTopoRefreshStmtTimeoutSeconds.getKeyName());
        props.remove(PropertyKey.tdsqlDirectProxyBlacklistTimeoutSeconds.getKeyName());
        props.remove(PropertyKey.tdsqlDirectReconnectProxyIntervalTimeSeconds.getKeyName());
        return props;
    }

    public TdsqlDirectDataSourceConfig(String dataSourceUuid) {
        this.dataSourceUuid = dataSourceUuid;
    }

    public String getDataSourceUuid() {
        return dataSourceUuid;
    }

    public TdsqlDirectReadWriteModeEnum getTdsqlDirectReadWriteMode() {
        return tdsqlDirectReadWriteMode;
    }

    public void setTdsqlDirectReadWriteMode(
            TdsqlDirectReadWriteModeEnum tdsqlDirectReadWriteMode) {
        this.tdsqlDirectReadWriteMode = tdsqlDirectReadWriteMode;
    }

    public TdsqlLoadBalanceStrategyEnum getTdsqlLoadBalanceStrategy() {
        return tdsqlLoadBalanceStrategy;
    }

    public void setTdsqlLoadBalanceStrategy(
            TdsqlLoadBalanceStrategyEnum tdsqlLoadBalanceStrategy) {
        this.tdsqlLoadBalanceStrategy = tdsqlLoadBalanceStrategy;
    }

    public Integer getTdsqlDirectTopoRefreshIntervalMillis() {
        return tdsqlDirectTopoRefreshIntervalMillis;
    }

    public void setTdsqlDirectTopoRefreshIntervalMillis(Integer tdsqlDirectTopoRefreshIntervalMillis) {
        this.tdsqlDirectTopoRefreshIntervalMillis = tdsqlDirectTopoRefreshIntervalMillis;
    }

    public Integer getTdsqlDirectMaxSlaveDelaySeconds() {
        return tdsqlDirectMaxSlaveDelaySeconds;
    }

    public void setTdsqlDirectMaxSlaveDelaySeconds(Integer tdsqlDirectMaxSlaveDelaySeconds) {
        this.tdsqlDirectMaxSlaveDelaySeconds = tdsqlDirectMaxSlaveDelaySeconds;
    }

    public Boolean getTdsqlDirectMasterCarryOptOfReadOnlyMode() {
        return tdsqlDirectMasterCarryOptOfReadOnlyMode;
    }

    public void setTdsqlDirectMasterCarryOptOfReadOnlyMode(Boolean tdsqlDirectMasterCarryOptOfReadOnlyMode) {
        this.tdsqlDirectMasterCarryOptOfReadOnlyMode = tdsqlDirectMasterCarryOptOfReadOnlyMode;
    }

    public Integer getTdsqlDirectTopoRefreshConnTimeoutMillis() {
        return tdsqlDirectTopoRefreshConnTimeoutMillis;
    }

    public void setTdsqlDirectTopoRefreshConnTimeoutMillis(Integer tdsqlDirectTopoRefreshConnTimeoutMillis) {
        this.tdsqlDirectTopoRefreshConnTimeoutMillis = tdsqlDirectTopoRefreshConnTimeoutMillis;
    }

    public Integer getTdsqlDirectTopoRefreshStmtTimeoutSeconds() {
        return tdsqlDirectTopoRefreshStmtTimeoutSeconds;
    }

    public void setTdsqlDirectTopoRefreshStmtTimeoutSeconds(Integer tdsqlDirectTopoRefreshStmtTimeoutSeconds) {
        this.tdsqlDirectTopoRefreshStmtTimeoutSeconds = tdsqlDirectTopoRefreshStmtTimeoutSeconds;
    }

    public Integer getTdsqlDirectCloseConnTimeoutMillis() {
        return tdsqlDirectCloseConnTimeoutMillis;
    }

    public void setTdsqlDirectCloseConnTimeoutMillis(Integer tdsqlDirectCloseConnTimeoutMillis) {
        this.tdsqlDirectCloseConnTimeoutMillis = tdsqlDirectCloseConnTimeoutMillis;
    }

    public Integer getTdsqlDirectProxyBlacklistTimeoutSeconds() {
        return tdsqlDirectProxyBlacklistTimeoutSeconds;
    }

    public void setTdsqlDirectProxyBlacklistTimeoutSeconds(Integer tdsqlDirectProxyBlacklistTimeoutSeconds) {
        this.tdsqlDirectProxyBlacklistTimeoutSeconds = tdsqlDirectProxyBlacklistTimeoutSeconds;
    }

    public Integer getTdsqlDirectReconnectProxyIntervalTimeSeconds() {
        return tdsqlDirectReconnectProxyIntervalTimeSeconds;
    }

    public void setTdsqlDirectReconnectProxyIntervalTimeSeconds(Integer tdsqlDirectReconnectProxyIntervalTimeSeconds) {
        this.tdsqlDirectReconnectProxyIntervalTimeSeconds = tdsqlDirectReconnectProxyIntervalTimeSeconds;
    }

    public List<HostInfo> getTdsqlDirectProxyHostInfoList() {
        return tdsqlDirectProxyHostInfoList;
    }

    public void setTdsqlDirectProxyHostInfoList(
            List<HostInfo> tdsqlDirectProxyHostInfoList) {
        this.tdsqlDirectProxyHostInfoList = tdsqlDirectProxyHostInfoList;
    }

    public void setTdsqlDirectOriginalPropertiesWithoutDirectMode(
            Properties tdsqlDirectOriginalPropertiesWithoutDirectMode) {
        this.tdsqlDirectOriginalPropertiesWithoutDirectMode = tdsqlDirectOriginalPropertiesWithoutDirectMode;
    }

    public ConnectionUrl getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(ConnectionUrl connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public TdsqlDirectTopologyServer getTopologyServer() {
        return topologyServer;
    }

    public void setTopologyServer(
            TdsqlDirectTopologyServer topologyServer) {
        this.topologyServer = topologyServer;
    }

    public TdsqlDirectCacheServer getCacheServer() {
        return cacheServer;
    }

    public void setCacheServer(TdsqlDirectCacheServer cacheServer) {
        this.cacheServer = cacheServer;
    }

    public TdsqlDirectScheduleServer getScheduleServer() {
        return scheduleServer;
    }

    public void setScheduleServer(
            TdsqlDirectScheduleServer scheduleServer) {
        this.scheduleServer = scheduleServer;
    }

    public TdsqlDirectConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(
            TdsqlDirectConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public TdsqlDirectFailoverHandler getFailoverHandler() {
        return failoverHandler;
    }

    public void setFailoverHandler(TdsqlDirectFailoverHandler failoverHandler) {
        this.failoverHandler = failoverHandler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectDataSourceConfig that = (TdsqlDirectDataSourceConfig) o;
        return Objects.equals(dataSourceUuid, that.dataSourceUuid)
                && tdsqlDirectReadWriteMode == that.tdsqlDirectReadWriteMode
                && tdsqlLoadBalanceStrategy == that.tdsqlLoadBalanceStrategy && Objects.equals(
                tdsqlDirectTopoRefreshIntervalMillis, that.tdsqlDirectTopoRefreshIntervalMillis)
                && Objects.equals(tdsqlDirectMaxSlaveDelaySeconds, that.tdsqlDirectMaxSlaveDelaySeconds)
                && Objects.equals(tdsqlDirectMasterCarryOptOfReadOnlyMode,
                that.tdsqlDirectMasterCarryOptOfReadOnlyMode) && Objects.equals(
                tdsqlDirectTopoRefreshConnTimeoutMillis, that.tdsqlDirectTopoRefreshConnTimeoutMillis)
                && Objects.equals(tdsqlDirectTopoRefreshStmtTimeoutSeconds,
                that.tdsqlDirectTopoRefreshStmtTimeoutSeconds) && Objects.equals(
                tdsqlDirectCloseConnTimeoutMillis, that.tdsqlDirectCloseConnTimeoutMillis) && Objects.equals(
                tdsqlDirectProxyBlacklistTimeoutSeconds, that.tdsqlDirectProxyBlacklistTimeoutSeconds)
                && Objects.equals(tdsqlDirectReconnectProxyIntervalTimeSeconds,
                that.tdsqlDirectReconnectProxyIntervalTimeSeconds) && Objects.equals(tdsqlDirectProxyHostInfoList,
                that.tdsqlDirectProxyHostInfoList) && Objects.equals(
                tdsqlDirectOriginalPropertiesWithoutDirectMode, that.tdsqlDirectOriginalPropertiesWithoutDirectMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSourceUuid, tdsqlDirectReadWriteMode, tdsqlLoadBalanceStrategy,
                tdsqlDirectTopoRefreshIntervalMillis, tdsqlDirectMaxSlaveDelaySeconds,
                tdsqlDirectMasterCarryOptOfReadOnlyMode, tdsqlDirectTopoRefreshConnTimeoutMillis,
                tdsqlDirectTopoRefreshStmtTimeoutSeconds, tdsqlDirectCloseConnTimeoutMillis,
                tdsqlDirectProxyBlacklistTimeoutSeconds, tdsqlDirectReconnectProxyIntervalTimeSeconds,
                tdsqlDirectProxyHostInfoList, tdsqlDirectOriginalPropertiesWithoutDirectMode);
    }

    @Override
    public String toString() {
        return "TdsqlDirectDataSourceConfig{" +
                "datasourceUuid='" + dataSourceUuid + '\'' +
                ", tdsqlDirectReadWriteMode=" + tdsqlDirectReadWriteMode +
                ", tdsqlLoadBalanceStrategy=" + tdsqlLoadBalanceStrategy +
                ", tdsqlDirectTopoRefreshIntervalMillis=" + tdsqlDirectTopoRefreshIntervalMillis +
                ", tdsqlDirectMaxSlaveDelaySeconds=" + tdsqlDirectMaxSlaveDelaySeconds +
                ", tdsqlDirectMasterCarryOptOfReadOnlyMode=" + tdsqlDirectMasterCarryOptOfReadOnlyMode +
                ", tdsqlDirectTopoRefreshConnTimeoutMillis=" + tdsqlDirectTopoRefreshConnTimeoutMillis +
                ", tdsqlDirectTopoRefreshStmtTimeoutSeconds=" + tdsqlDirectTopoRefreshStmtTimeoutSeconds +
                ", tdsqlDirectCloseConnTimeoutMillis=" + tdsqlDirectCloseConnTimeoutMillis +
                ", tdsqlDirectProxyBlacklistTimeoutSeconds=" + tdsqlDirectProxyBlacklistTimeoutSeconds +
                ", tdsqlDirectReconnectProxyIntervalTimeSeconds=" + tdsqlDirectReconnectProxyIntervalTimeSeconds +
                ", tdsqlDirectProxyHostInfoList=" + tdsqlDirectProxyHostInfoList +
                ", tdsqlDirectOriginalPropertiesWithoutDirectMode=" + tdsqlDirectOriginalPropertiesWithoutDirectMode +
                '}';
    }
}
