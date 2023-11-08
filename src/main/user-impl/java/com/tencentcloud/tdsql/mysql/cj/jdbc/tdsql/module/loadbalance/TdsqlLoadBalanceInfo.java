package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.loadbalance;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logDebug;

/**
 * <p>TDSQL-MySQL独有的负载均衡信息记录类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLoadBalanceInfo {

    /**
     * 全局唯一的DataSourceUuid
     */
    private String datasourceUuid;
    private List<TdsqlHostInfo> tdsqlHostInfoList;
    private String tdsqlLoadBalanceStrategy;
    private List<Integer> tdsqlLoadBalanceWeightFactorList;
    private boolean tdsqlLoadBalanceHeartbeatMonitorEnable;
    private int tdsqlLoadBalanceHeartbeatIntervalTimeMillis;
    private int tdsqlLoadBalanceHeartbeatMaxErrorRetries;
    private int tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis;

    public void setDatasourceUuid(String datasourceUuid) {
        this.datasourceUuid = datasourceUuid;
    }

    public String getDatasourceUuid() {
        return datasourceUuid;
    }

    public List<TdsqlHostInfo> getTdsqlHostInfoList() {
        return tdsqlHostInfoList;
    }

    /**
     * <p>
     * 在设置{@link TdsqlHostInfo}对象列表的同时，生成全局唯一的DataSourceUuid
     * 该DataSourceUuid是由全部的IP和Port地址排序后拼接而成
     * </p>
     *
     * @param tdsqlHostInfoList {@link TdsqlHostInfo}对象列表
     */
    public void setTdsqlHostInfoList(List<TdsqlHostInfo> tdsqlHostInfoList, ConnectionUrl connectionUrl) {
        this.tdsqlHostInfoList = tdsqlHostInfoList;
        this.datasourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        logDebug("current connection uuid: " + this.datasourceUuid);

        for (TdsqlHostInfo tdsqlHostInfo : this.tdsqlHostInfoList) {
            tdsqlHostInfo.setOwnerUuid(this.datasourceUuid);
        }
    }

    public String getTdsqlLoadBalanceStrategy() {
        return tdsqlLoadBalanceStrategy;
    }

    public void setTdsqlLoadBalanceStrategy(String tdsqlLoadBalanceStrategy) {
        this.tdsqlLoadBalanceStrategy = tdsqlLoadBalanceStrategy;
    }

    public void setTdsqlLoadBalanceWeightFactorList(List<Integer> tdsqlLoadBalanceWeightFactorList) {
        this.tdsqlLoadBalanceWeightFactorList = tdsqlLoadBalanceWeightFactorList;
    }

    public boolean isTdsqlLoadBalanceHeartbeatMonitorEnable() {
        return tdsqlLoadBalanceHeartbeatMonitorEnable;
    }

    public void setTdsqlLoadBalanceHeartbeatMonitorEnable(boolean tdsqlLoadBalanceHeartbeatMonitorEnable) {
        this.tdsqlLoadBalanceHeartbeatMonitorEnable = tdsqlLoadBalanceHeartbeatMonitorEnable;
    }

    public int getTdsqlLoadBalanceHeartbeatIntervalTimeMillis() {
        return tdsqlLoadBalanceHeartbeatIntervalTimeMillis;
    }

    public void setTdsqlLoadBalanceHeartbeatIntervalTimeMillis(int tdsqlLoadBalanceHeartbeatIntervalTimeMillis) {
        this.tdsqlLoadBalanceHeartbeatIntervalTimeMillis = tdsqlLoadBalanceHeartbeatIntervalTimeMillis;
    }

    public int getTdsqlLoadBalanceHeartbeatMaxErrorRetries() {
        return tdsqlLoadBalanceHeartbeatMaxErrorRetries;
    }

    public void setTdsqlLoadBalanceHeartbeatMaxErrorRetries(int tdsqlLoadBalanceHeartbeatMaxErrorRetries) {
        this.tdsqlLoadBalanceHeartbeatMaxErrorRetries = tdsqlLoadBalanceHeartbeatMaxErrorRetries;
    }

    public int getTdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis() {
        return tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis;
    }

    public void setTdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis(
            int tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis) {
        this.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis = tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis;
    }

    public Set<String> getIpPortSet() {
        Set<String> ipPortSet = new LinkedHashSet<>();
        for (TdsqlHostInfo hostInfo : this.getTdsqlHostInfoList()) {
            ipPortSet.add(hostInfo.getHostPortPair());
        }
        return ipPortSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlLoadBalanceInfo that = (TdsqlLoadBalanceInfo) o;
        return tdsqlLoadBalanceHeartbeatMonitorEnable == that.tdsqlLoadBalanceHeartbeatMonitorEnable
                && tdsqlLoadBalanceHeartbeatIntervalTimeMillis == that.tdsqlLoadBalanceHeartbeatIntervalTimeMillis
                && tdsqlLoadBalanceHeartbeatMaxErrorRetries == that.tdsqlLoadBalanceHeartbeatMaxErrorRetries
                && tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis
                == that.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis
                && datasourceUuid.equals(that.datasourceUuid)
                && tdsqlHostInfoList.equals(that.tdsqlHostInfoList)
                && tdsqlLoadBalanceWeightFactorList.equals(that.tdsqlLoadBalanceWeightFactorList)
                && tdsqlLoadBalanceStrategy.equalsIgnoreCase(that.tdsqlLoadBalanceStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, tdsqlHostInfoList, tdsqlLoadBalanceStrategy,
                tdsqlLoadBalanceWeightFactorList, tdsqlLoadBalanceHeartbeatMonitorEnable,
                tdsqlLoadBalanceHeartbeatIntervalTimeMillis, tdsqlLoadBalanceHeartbeatMaxErrorRetries,
                tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis);
    }

    @Override
    public String toString() {
        return "TdsqlLoadBalanceInfo{" +
                "datasourceUuid='" + datasourceUuid + '\'' +
                ", tdsqlHostInfoList=" + tdsqlHostInfoList +
                ", tdsqlLoadBalanceStrategy='" + tdsqlLoadBalanceStrategy + '\'' +
                ", tdsqlLoadBalanceWeightFactorList=" + tdsqlLoadBalanceWeightFactorList +
                ", tdsqlLoadBalanceHeartbeatMonitorEnable=" + tdsqlLoadBalanceHeartbeatMonitorEnable +
                ", tdsqlLoadBalanceHeartbeatIntervalTimeMillis=" + tdsqlLoadBalanceHeartbeatIntervalTimeMillis +
                ", tdsqlLoadBalanceHeartbeatMaxErrorRetries=" + tdsqlLoadBalanceHeartbeatMaxErrorRetries +
                ", tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis="
                + tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis +
                '}';
    }
}
