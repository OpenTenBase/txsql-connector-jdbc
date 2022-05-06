package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import java.sql.SQLException;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionProxy {

    public static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        JdbcConnection newConnection;

        topoServer.initialize(connectionUrl);

        topoServer.lock.lock();
        try {
            HostInfo choice = new TdsqlDirectLoadBalanceStrategy().choice();
            newConnection = TdsqlDirectConnectionManager.getInstance().pickNewConnection(choice);
        } finally {
            topoServer.lock.unlock();
        }
        return newConnection;
    }
}
