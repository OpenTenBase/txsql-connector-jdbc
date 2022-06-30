package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

/**
 * @author dorianzhang@tencent.com
 */
public final class TdsqlConst {

    public static final String TDSQL_DIRECT_READ_WRITE_MODE_RW = "rw";
    public static final String TDSQL_DIRECT_READ_WRITE_MODE_RO = "ro";
    public static final Integer TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS = 0;
    public static final Integer TDSQL_DIRECT_TOPO_REFRESH_INTERVAL_MILLIS = 1000;
    public static final String TDSQL_DIRECT_SHOW_ROUTES_SQL = "/*proxy*/ show routes";
    public static final String TDSQL_DIRECT_ROUTE_ACTIVE_TRUE = "0";
    public static final String TDSQL_DIRECT_ROUTE_WATCH_TRUE = "1";
    public static final Integer TDSQL_DIRECT_TOPO_REFRESH_CONN_TIMEOUT_MILLIS = 1000;
    public static final Integer TDSQL_DIRECT_CLOSE_CONN_TIMEOUT_MILLIS = 1000;
    public static final Integer TDSQL_DIRECT_TOPO_REFRESH_STMT_TIMEOUT_SECONDS = 1;
    public static final String TDSQL_DIRECT_TOPO_COLUMN_CLUSTER_NAME = "cluster_name";
    public static final String TDSQL_DIRECT_TOPO_COLUMN_MASTER_IP = "master_ip";
    public static final String TDSQL_DIRECT_TOPO_COLUMN_SLAVE_IP_LIST = "slave_iplist";
}
