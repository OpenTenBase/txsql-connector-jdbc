package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.loadbalance;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

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

    private static final String QUESTION_MARK = "?";
    private static final String PLUS_MARK = "+";
    private static final String AND_MARK = "&";
    private static final String EQUAL_MARK = "=";
    private static final String LEFT_CURLY_BRACES = "{";
    private static final String RIGHT_CURLY_BRACES = "}";

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
        logInfo("current connection uuid: " + this.datasourceUuid);

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

//    /**
//     * <p>
//     * 解析DataSourceUuid的值，获取其中的IP和端口列表
//     * </p>
//     *
//     * @param datasourceUuid DataSourceUuid
//     * @return IP和端口字符串列表
//     */
//    public static Set<String> parseDatasourceUuid(String datasourceUuid) {
//        String ipPortDbStr = datasourceUuid;
//
//        // 去掉URL参数
//        if (datasourceUuid.contains(QUESTION_MARK)) {
//            ipPortDbStr = datasourceUuid.substring(0, datasourceUuid.indexOf(QUESTION_MARK));
//        }
//
//        // 去掉数据库名称
//        String ipPortStr = ipPortDbStr.substring(0, ipPortDbStr.lastIndexOf(PLUS_MARK));
//        return new LinkedHashSet<>(StringUtils.split(ipPortStr, "\\+", true));
//    }

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
