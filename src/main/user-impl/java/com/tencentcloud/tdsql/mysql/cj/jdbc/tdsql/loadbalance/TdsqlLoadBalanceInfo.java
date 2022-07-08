package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

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
    private List<Integer> tdsqlLoadBalanceWeightFactorList;
    private boolean tdsqlLoadBalanceHeartbeatMonitor;
    private int tdsqlLoadBalanceHeartbeatIntervalTime;
    private int tdsqlLoadBalanceMaximumErrorRetries;

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
    public void setTdsqlHostInfoList(List<TdsqlHostInfo> tdsqlHostInfoList) {
        List<String> hostPortPairList = new ArrayList<>(tdsqlHostInfoList.size());
        for (TdsqlHostInfo tdsqlHostInfo : tdsqlHostInfoList) {
            hostPortPairList.add(tdsqlHostInfo.getHostPortPair());
        }
        Collections.sort(hostPortPairList);
        StringJoiner uuidJoiner = new StringJoiner("+");
        for (String hostPort : hostPortPairList) {
            uuidJoiner.add(hostPort);
        }

        this.tdsqlHostInfoList = tdsqlHostInfoList;
        this.datasourceUuid = uuidJoiner.toString();

        for (TdsqlHostInfo tdsqlHostInfo : this.tdsqlHostInfoList) {
            tdsqlHostInfo.setOwnerUuid(this.datasourceUuid);
        }
    }

    public void setTdsqlLoadBalanceWeightFactorList(List<Integer> tdsqlLoadBalanceWeightFactorList) {
        this.tdsqlLoadBalanceWeightFactorList = tdsqlLoadBalanceWeightFactorList;
    }

    public boolean isTdsqlLoadBalanceHeartbeatMonitor() {
        return tdsqlLoadBalanceHeartbeatMonitor;
    }

    public void setTdsqlLoadBalanceHeartbeatMonitor(boolean tdsqlLoadBalanceHeartbeatMonitor) {
        this.tdsqlLoadBalanceHeartbeatMonitor = tdsqlLoadBalanceHeartbeatMonitor;
    }

    public int getTdsqlLoadBalanceHeartbeatIntervalTime() {
        return tdsqlLoadBalanceHeartbeatIntervalTime;
    }

    public void setTdsqlLoadBalanceHeartbeatIntervalTime(int tdsqlLoadBalanceHeartbeatIntervalTime) {
        this.tdsqlLoadBalanceHeartbeatIntervalTime = tdsqlLoadBalanceHeartbeatIntervalTime;
    }

    public int getTdsqlLoadBalanceMaximumErrorRetries() {
        return tdsqlLoadBalanceMaximumErrorRetries;
    }

    public void setTdsqlLoadBalanceMaximumErrorRetries(int tdsqlLoadBalanceMaximumErrorRetries) {
        this.tdsqlLoadBalanceMaximumErrorRetries = tdsqlLoadBalanceMaximumErrorRetries;
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
        return tdsqlLoadBalanceHeartbeatMonitor == that.tdsqlLoadBalanceHeartbeatMonitor
                && tdsqlLoadBalanceHeartbeatIntervalTime == that.tdsqlLoadBalanceHeartbeatIntervalTime
                && tdsqlLoadBalanceMaximumErrorRetries == that.tdsqlLoadBalanceMaximumErrorRetries
                && datasourceUuid.equals(that.datasourceUuid)
                && tdsqlHostInfoList.equals(that.tdsqlHostInfoList)
                && tdsqlLoadBalanceWeightFactorList.equals(that.tdsqlLoadBalanceWeightFactorList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, tdsqlHostInfoList, tdsqlLoadBalanceWeightFactorList,
                tdsqlLoadBalanceHeartbeatMonitor, tdsqlLoadBalanceHeartbeatIntervalTime,
                tdsqlLoadBalanceMaximumErrorRetries);
    }

    @Override
    public String toString() {
        return "TdsqlLoadBalanceInfo{" +
                "hostUuid='" + datasourceUuid + '\'' +
                ", hostsList=" + tdsqlHostInfoList +
                ", haLoadBalanceWeightFactorList=" + tdsqlLoadBalanceWeightFactorList +
                ", haLoadBalanceHeartbeatMonitor=" + tdsqlLoadBalanceHeartbeatMonitor +
                ", haLoadBalanceHeartbeatIntervalTime=" + tdsqlLoadBalanceHeartbeatIntervalTime +
                ", haLoadBalanceMaximumErrorRetries=" + tdsqlLoadBalanceMaximumErrorRetries +
                '}';
    }
}
