package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlLoadBalanceConst {

    public static final String TDSQL_LOAD_BALANCE_STRATEGY_SED = "sed";
    public static final Integer TDSQL_LOAD_BALANCE_BLACKLIST_TIMEOUT_MILLIS = 5000;
    public static final Boolean TDSQL_LOAD_BALANCE_HEARTBEAT_MONITOR_ENABLE = true;
    public static final Integer LOAD_BALANCE_HEARTBEAT_INTERVAL_TIME_MILLIS = 3000;
    public static final Integer LOAD_BALANCE_MAXIMUM_ERROR_RETRIES_ONE = 1;
}
