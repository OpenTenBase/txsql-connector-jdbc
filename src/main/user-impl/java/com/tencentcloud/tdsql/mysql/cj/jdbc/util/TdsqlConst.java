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
    public static final Integer TDSQL_SHOW_ROUTES_CONNECTION_TIMEOUT_MILLISECONDS = 5000;
    public static final Integer TDSQL_CLOSE_CONNECTION_TIMEOUT_MILLISECONDS = 5000;
    public static final Integer TDSQL_SHOW_ROUTES_STATEMENT_TIMEOUT_SECONDS = 5;
    public static final String TDSQL_SHOW_ROUTES_COLUMN_CLUSTER_NAME = "cluster_name";
    public static final String TDSQL_SHOW_ROUTES_COLUMN_MASTER_IP = "master_ip";
    public static final String TDSQL_SHOW_ROUTES_COLUMN_SLAVE_IP_LIST = "slave_iplist";
}
