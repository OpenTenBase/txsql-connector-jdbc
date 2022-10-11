package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
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
        StringJoiner ipPortDbJoiner = new StringJoiner(PLUS_MARK);
        for (String hostPort : hostPortPairList) {
            ipPortDbJoiner.add(hostPort);
        }

        // 继续拼装数据库名称
        TdsqlHostInfo info = tdsqlHostInfoList.get(0);
        ipPortDbJoiner.add(info.getDatabase());

        // 继续拼装URL参数
        StringJoiner propJoiner = new StringJoiner(AND_MARK, LEFT_CURLY_BRACES, RIGHT_CURLY_BRACES);
        for (Entry<String, String> entry : info.getHostProperties().entrySet()) {
            propJoiner.add(entry.getKey() + EQUAL_MARK + entry.getValue());
        }

        // 最终的UUID
        String uuid = ipPortDbJoiner + QUESTION_MARK + propJoiner;

        this.tdsqlHostInfoList = tdsqlHostInfoList;
        this.datasourceUuid = uuid;

        for (TdsqlHostInfo tdsqlHostInfo : this.tdsqlHostInfoList) {
            tdsqlHostInfo.setOwnerUuid(this.datasourceUuid);
        }
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

    /**
     * <p>
     * 解析DataSourceUuid的值，获取其中的IP和端口列表
     * </p>
     *
     * @param datasourceUuid DataSourceUuid
     * @return IP和端口字符串列表
     */
    public static Set<String> parseDatasourceUuid(String datasourceUuid) {
        String ipPortDbStr = datasourceUuid;

        // 去掉URL参数
        if (datasourceUuid.contains(QUESTION_MARK)) {
            ipPortDbStr = datasourceUuid.substring(0, datasourceUuid.indexOf(QUESTION_MARK));
        }

        // 去掉数据库名称
        String ipPortStr = ipPortDbStr.substring(0, ipPortDbStr.lastIndexOf(PLUS_MARK));
        return new LinkedHashSet<>(StringUtils.split(ipPortStr, "\\+", true));
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
                && tdsqlLoadBalanceWeightFactorList.equals(that.tdsqlLoadBalanceWeightFactorList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, tdsqlHostInfoList, tdsqlLoadBalanceWeightFactorList,
                tdsqlLoadBalanceHeartbeatMonitorEnable, tdsqlLoadBalanceHeartbeatIntervalTimeMillis,
                tdsqlLoadBalanceHeartbeatMaxErrorRetries, tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis);
    }

    @Override
    public String toString() {
        return "TdsqlLoadBalanceInfo{" +
                "datasourceUuid='" + datasourceUuid + '\'' +
                ", tdsqlHostInfoList=" + tdsqlHostInfoList +
                ", tdsqlLoadBalanceWeightFactorList=" + tdsqlLoadBalanceWeightFactorList +
                ", tdsqlLoadBalanceHeartbeatMonitorEnable=" + tdsqlLoadBalanceHeartbeatMonitorEnable +
                ", tdsqlLoadBalanceHeartbeatIntervalTimeMillis=" + tdsqlLoadBalanceHeartbeatIntervalTimeMillis +
                ", tdsqlLoadBalanceHeartbeatMaxErrorRetries=" + tdsqlLoadBalanceHeartbeatMaxErrorRetries +
                ", tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis="
                + tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis +
                '}';
    }
}
