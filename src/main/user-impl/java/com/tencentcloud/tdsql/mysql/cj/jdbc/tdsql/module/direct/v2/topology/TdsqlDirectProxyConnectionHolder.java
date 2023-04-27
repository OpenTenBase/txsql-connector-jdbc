package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;

/**
 * <p>TDSQL专属，直连模式Proxy连接包装类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectProxyConnectionHolder {

    private final JdbcConnection jdbcConnection;
    private final long holdTimeMillis;

    public TdsqlDirectProxyConnectionHolder(JdbcConnection jdbcConnection, long holdTimeMillis) {
        this.jdbcConnection = jdbcConnection;
        this.holdTimeMillis = holdTimeMillis;
    }

    public JdbcConnection getJdbcConnection() {
        return jdbcConnection;
    }

    public long getHoldTimeMillis() {
        return holdTimeMillis;
    }
}
