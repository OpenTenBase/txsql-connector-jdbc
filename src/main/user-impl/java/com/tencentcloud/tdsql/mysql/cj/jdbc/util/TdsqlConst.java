package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

/**
 * @author dorianzhang@tencent.com
 */
public final class TdsqlConst {

    public static final String TDSQL_READ_WRITE_MODE_RW = "rw";
    public static final String TDSQL_READ_WRITE_MODE_RO = "ro";
    public static final Integer TDSQL_MAX_SLAVE_DELAY_DEFAULT_VALUE = 0;
    public static final Integer TDSQL_PROXY_TOPO_REFRESH_INTERVAL_DEFAULT_VALUE = 1000;

    public static final String TDSQL_SHOW_ROUTES_SQL = "/*proxy*/ show routes";
    public static final String TDSQL_ROUTE_ACTIVE_TRUE = "0";
    public static final String TDSQL_ROUTE_WATCH_TRUE = "1";

    public static final Integer TDSQL_SHOW_ROUTES_TIMEOUT_SECONDS = 15;
    public static final Integer TDSQL_SHOW_ROUTES_CONN_TIMEOUT = TDSQL_SHOW_ROUTES_TIMEOUT_SECONDS * 2 * 1000;
}
