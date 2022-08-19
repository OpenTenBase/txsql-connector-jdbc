package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

//心跳检测的参数
public class TdsqlDirectHeartbeatAttributes {
    private boolean tdsqlDirectHeartbeatMonitorEnable;
    private int tdsqlDirectHeartbeatIntervalTimeMillis;
    private int tdsqlDirectHeartbeatMaxErrorRetries;
    private int tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis;

    /**
     * 此处进行心跳检测
     * 参数一：是否开启心跳检测
     * 参数二：心跳检测的重试次数
     * 参数三：心跳检测的间隔
     * tdsqlloadbalance的信息
     *     private String datasourceUuid;
     *     private List<TdsqlHostInfo> tdsqlHostInfoList;
     *     private List<Integer> tdsqlLoadBalanceWeightFactorList;
     *     private boolean tdsqlLoadBalanceHeartbeatMonitorEnable;
     *     private int tdsqlLoadBalanceHeartbeatIntervalTimeMillis;
     *     private int tdsqlLoadBalanceHeartbeatMaxErrorRetries;
     *     private int tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis;
     */
    public TdsqlDirectHeartbeatAttributes(){
    }

    public boolean isTdsqlDirectHeartbeatMonitorEnable() {
        return this.tdsqlDirectHeartbeatMonitorEnable;
    }

    public int getTdsqlDirectHeartbeatErrorRetryIntervalTimeMillis() {
        return this.tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis;
    }

    public int getTdsqlLoadDirectHeartbeatMaxErrorRetries() {
        return this.tdsqlDirectHeartbeatMaxErrorRetries;
    }

    public int getTdsqlDirectHeartbeatIntervalTimeMillis() {
        return this.tdsqlDirectHeartbeatIntervalTimeMillis;
    }

    public void setTdsqlDirectHeartbeatMonitorEnable(boolean tdsqlDirectHeartbeatMonitorEnable) {
        this.tdsqlDirectHeartbeatMonitorEnable = tdsqlDirectHeartbeatMonitorEnable;
    }

    public void setTdsqlDirectHeartbeatIntervalTimeMillis(int tdsqlDirectHeartbeatIntervalTimeMillis) {
        this.tdsqlDirectHeartbeatIntervalTimeMillis = tdsqlDirectHeartbeatIntervalTimeMillis;
    }

    public void setTdsqlDirectHeartbeatMaxErrorRetries(int tdsqlLoadDirectHeartbeatMaxErrorRetries) {
        this.tdsqlDirectHeartbeatMaxErrorRetries = tdsqlLoadDirectHeartbeatMaxErrorRetries;
    }

    public void setTdsqlDirectHeartbeatErrorRetryIntervalTimeMillis(int tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis) {
        this.tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis = tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis;
    }
}
