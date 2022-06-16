package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlLoadBalanceConst {

    public static final String TDSQL_LOAD_BALANCE_STRATEGY_DEFAULT_VALUE = "sed";
    public static final Integer TDSQL_LOAD_BALANCE_BLACKLIST_TIMEOUT_DEFAULT_VALUE = 5000;
    public static final Boolean TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_DEFAULT_VALUE = true;
    public static final Integer LOAD_BALANCE_HEARTBEAT_INTERVAL_TIME_DEFAULT_VALUE = 3000;
    public static final Integer LOAD_BALANCE_MAXIMUM_ERROR_RETRIES_DEFAULT_VALUE = 1;
}
