package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance;

/**
 * <p>负载均衡模式常量类</p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlLoadBalanceConst {

    public static final String DEFAULT_TDSQL_LOAD_BALANCE_STRATEGY = "sed";
    public static final Boolean DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE = true;
    public static final Integer DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_INTERVAL_TIME_MILLIS = 3000;
    public static final Integer DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_MAX_ERROR_RETRIES = 1;
    public static final Integer DEFAULT_TDSQL_LOAD_BALANCE_HEARTBEAT_ERROR_RETRY_INTERVAL_TIME_MILLIS = 5000;
    public static final String TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_TRUE = "true";
    public static final String TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE_FALSE = "false";
}
