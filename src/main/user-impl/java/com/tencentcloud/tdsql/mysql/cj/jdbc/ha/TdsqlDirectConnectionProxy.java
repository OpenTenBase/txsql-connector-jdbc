package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionProxy {

    public synchronized static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        TdsqlDirectTopoServer.getInstance().initialize(connectionUrl);
        TdsqlLoadBalanceStrategy balancer = new TdsqlDirectLoadBalanceStrategy();
        HostInfo choice = balancer.choice(new ArrayList<HostInfo>() {{
            add(connectionUrl.getMainHost());
        }});
        return TdsqlDirectConnectionManager.getInstance().pickNewConnection(choice);
    }
}
