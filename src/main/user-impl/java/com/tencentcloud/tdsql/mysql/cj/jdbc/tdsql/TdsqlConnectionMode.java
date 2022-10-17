package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

/**
 * <p>TDSQL-NySQL专属的，数据库连接模式</p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlConnectionMode {

    /**
     * 负载均衡模式
     */
    LOAD_BALANCE,

    /**
     * 直连模式
     */
    DIRECT,

    /**
     * 未知模式
     */
    UNKNOWN
}
