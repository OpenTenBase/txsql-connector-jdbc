package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct;

/**
 * <p>TDSQL专属，直连模式常量类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConst {

    public static final String TDSQL_DIRECT_READ_WRITE_MODE_RW = "rw";
    public static final String TDSQL_DIRECT_READ_WRITE_MODE_RO = "ro";
    public static final String DEFAULT_TDSQL_DIRECT_READ_WRITE_MODE = TDSQL_DIRECT_READ_WRITE_MODE_RW;
    public static final Integer DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_INTERVAL_MILLIS = 1000;
    public static final Integer DEFAULT_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS = 0;
    public static final Integer MAXIMUM_TDSQL_DIRECT_MAX_SLAVE_DELAY_SECONDS = 100000;
    public static final Integer DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_CONNECT_TIMEOUT_MILLIS = 2000;
    public static final Integer DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_CONN_SOCKET_TIMEOUT_MILLIS = 2000;
    public static final Integer DEFAULT_TDSQL_DIRECT_TOPO_REFRESH_STMT_TIMEOUT_SECONDS = 1;
    public static final Integer DEFAULT_TDSQL_DIRECT_CLOSE_CONN_TIMEOUT_MILLIS = 2000;
    public static final Integer DEFAULT_TDSQL_DIRECT_PROXY_BLACKLIST_TIMEOUT_SECONDS = 60;
    public static final Integer DEFAULT_TDSQL_DIRECT_RECONNECT_PROXY_INTERVAL_TIME_SECONDS = 60 * 10;
    public static final Integer MINIMUM_TDSQL_DIRECT_RECONNECT_PROXY_INTERVAL_TIME_SECONDS = 30;
    public static final Integer MAXIMUM_TDSQL_DIRECT_RECONNECT_PROXY_INTERVAL_TIME_SECONDS = 60 * 10;
    public static final Integer DEFAULT_TDSQL_DIRECT_PROXY_CONNECT_MAX_IDLE_TIME_SECONDS = 60;
    public static final Integer MAXIMUM_TDSQL_DIRECT_PROXY_CONNECT_MAX_IDLE_TIME_SECONDS = 60 * 60;
    public static final Integer MINIMUM_TDSQL_DIRECT_PROXY_CONNECT_MAX_IDLE_TIME_SECONDS = 30;
    public static final String DEFAULT_CONNECTION_TIME_ZONE = "GMT+8";
    public static final String TDSQL_DIRECT_REFRESH_TOPOLOGY_SQL = "/*proxy*/ show routes";
    public static final String TDSQL_DIRECT_ROUTE_ACTIVE_TRUE = "0";
    public static final String TDSQL_DIRECT_ROUTE_WATCH_TRUE = "1";
    public static final String TDSQL_DIRECT_TOPOLOGY_COLUMN_CLUSTER_NAME = "cluster_name";
    public static final String TDSQL_DIRECT_TOPOLOGY_COLUMN_MASTER_IP = "master_ip";
    public static final String TDSQL_DIRECT_TOPOLOGY_COLUMN_SLAVE_IP_LIST = "slave_iplist";
}
