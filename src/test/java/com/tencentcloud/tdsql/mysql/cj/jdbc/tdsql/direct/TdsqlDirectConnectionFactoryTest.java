package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlDirectLoadBalanceStrategyFactory;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;

class TdsqlDirectConnectionFactoryTest {
    private TdsqlLoadBalanceStrategy balancer;
    public void TestFunction() throws SQLException {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
        refreshLock.readLock().lock();
        String strategy = "Lc";
        this.balancer = TdsqlDirectLoadBalanceStrategyFactory.getInstance().getStrategyInstance(strategy);
        JdbcConnection newConnection;
        try {
            newConnection = TdsqlDirectConnectionManager.getInstance()
                    .createNewConnection(this.balancer, true);
        } finally {
            refreshLock.readLock().unlock();
        }

    }

}