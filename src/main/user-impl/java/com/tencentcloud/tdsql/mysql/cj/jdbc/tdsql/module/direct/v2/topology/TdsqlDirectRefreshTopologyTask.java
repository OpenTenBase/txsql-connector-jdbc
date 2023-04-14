package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.TDSQL_DIRECT_REFRESH_TOPOLOGY_SQL;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.TDSQL_DIRECT_TOPOLOGY_COLUMN_CLUSTER_NAME;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.TDSQL_DIRECT_TOPOLOGY_COLUMN_MASTER_IP;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.TDSQL_DIRECT_TOPOLOGY_COLUMN_SLAVE_IP_LIST;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectRefreshTopologyException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>TDSQL专属，直连模式拓扑信息刷新任务</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectRefreshTopologyTask implements Runnable {

    private final String dataSourceUuid;
    private final TdsqlDirectTopologyServer topoServer;
    private final TdsqlDirectCacheServer cacheServer;
    private final List<String> unmodifiableHostPortList;
    private final Map<String, TdsqlDirectProxyConnectionHolder> unmodifiableLiveConnectionMap;

    public Throwable getLastException() {
        return lastException;
    }

    private Throwable lastException;

    public TdsqlDirectRefreshTopologyTask(TdsqlDirectDataSourceConfig dataSourceConfig,
            List<String> unmodifiableHostPortList,
            Map<String, TdsqlDirectProxyConnectionHolder> unmodifiableLiveConnectionMap) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.topoServer = dataSourceConfig.getTopologyServer();
        this.cacheServer = dataSourceConfig.getCacheServer();
        this.unmodifiableHostPortList = unmodifiableHostPortList;
        this.unmodifiableLiveConnectionMap = unmodifiableLiveConnectionMap;
    }

    @Override
    public void run() {
        try {
            TdsqlDirectProxyConnectionHolder proxyConnectionHolder;
            // 随机选择一个Proxy并建立连接
            do {
                proxyConnectionHolder = this.choiceRandomProxyConnection();
                // 判断选择的Proxy连接是否需要重连
                if (this.topoServer.needReconnectProxy(proxyConnectionHolder.getHoldTimeMillis())) {
                    this.topoServer.closeProxyConnection(proxyConnectionHolder.getJdbcConnection().getHostPortPair());
                    logInfo(this.dataSourceUuid,
                            Messages.getString("TdsqlDirectRefreshTopologyMessage.ReconnectProxyConnection",
                                    new Object[]{proxyConnectionHolder.getJdbcConnection().getHostPortPair()}));
                }
                // 保证Proxy连接可用
            } while (!this.topoServer.isProxyConnectionValid(proxyConnectionHolder.getJdbcConnection()));

            // 刷新拓扑
            this.refreshTopology(proxyConnectionHolder.getJdbcConnection());
        } catch (Throwable t) {
            if (t.getCause() != null) {
                this.lastException = t.getCause();
            } else {
                this.lastException = t;
            }
            TdsqlLoggerFactory.logError(this.dataSourceUuid, t.getMessage(), t);
        }
    }

    private TdsqlDirectProxyConnectionHolder choiceRandomProxyConnection() {
        int numHosts = this.unmodifiableHostPortList.size();
        List<String> allowList = new ArrayList<>(numHosts);
        allowList.addAll(this.unmodifiableHostPortList);

        Map<String, Long> blacklist = this.topoServer.getProxyBlacklist();
        allowList.removeAll(blacklist.keySet());
        if (allowList.isEmpty()) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                    Messages.getString("TdsqlDirectRefreshTopologyException.NoProxyHostsConfiguredInConnectionUrl"));
        }

        Map<String, Integer> allowListIndexMap = this.getArrayIndexMap(allowList);

        TdsqlDirectProxyConnectionHolder connectionHolder;
        for (Iterator<String> it = allowList.iterator(); it.hasNext(); ) {
            int random = (int) Math.floor((Math.random() * allowList.size()));
            String hostPortSpec = allowList.get(random);

            connectionHolder = this.unmodifiableLiveConnectionMap.get(hostPortSpec);
            if (connectionHolder == null) {
                try {
                    connectionHolder = this.topoServer.createConnectionForHost(hostPortSpec);
                } catch (SQLException e) {

                    TdsqlLoggerFactory.logError(this.dataSourceUuid, Messages.getString(
                            "TdsqlDirectRefreshTopologyMessage.FailedToEstablishConnectionWithOneProxy",
                            new Object[]{hostPortSpec}));
                    // 排除此主机，避免再次被选中
                    Integer allowListIndex = allowListIndexMap.get(hostPortSpec);
                    if (allowListIndex != null) {
                        allowList.remove(allowListIndex.intValue());
                        allowListIndexMap = this.getArrayIndexMap(allowList);
                    }
                    this.topoServer.addToBlacklist(hostPortSpec);

                    if (allowList.isEmpty()) {
                        // 底层sql可能连接会包一层错误，无法返回根本原因，因此选择getCause
                        throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                                Messages.getString(
                                        "TdsqlDirectRefreshTopologyException.FailedToEstablishConnectionWithAllProxies"),
                                e);
                    }
                    continue;
                }
            }
            logInfo(this.dataSourceUuid,
                    Messages.getString("TdsqlDirectRefreshTopologyMessage.ChoiceOneProxy", new Object[]{hostPortSpec}));
            return connectionHolder;
        }
        // we won't get here, compiler can't tell
        return new TdsqlDirectProxyConnectionHolder(null, 0L);
    }

    private void refreshTopology(JdbcConnection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.setQueryTimeout(this.topoServer.getDataSourceConfig().getTdsqlDirectTopoRefreshStmtTimeoutSeconds());
            try (ResultSet rs = stmt.executeQuery(TDSQL_DIRECT_REFRESH_TOPOLOGY_SQL)) {
                while (rs.next()) {
                    String clusterName = rs.getString(TDSQL_DIRECT_TOPOLOGY_COLUMN_CLUSTER_NAME);
                    String masterInfoStr = rs.getString(TDSQL_DIRECT_TOPOLOGY_COLUMN_MASTER_IP);
                    String slavesInfoStr = rs.getString(TDSQL_DIRECT_TOPOLOGY_COLUMN_SLAVE_IP_LIST);
                    TdsqlDirectTopologyInfo topologyInfo = new TdsqlDirectTopologyInfo(
                            this.topoServer.getDataSourceConfig().getDataSourceUuid(), clusterName,
                            this.topoServer.getDataSourceConfig().getTdsqlDirectReadWriteMode(),
                            this.topoServer.getDataSourceConfig().getTdsqlDirectMasterCarryOptOfReadOnlyMode(),
                            masterInfoStr, slavesInfoStr);
                    this.cacheServer.compareAndCache(topologyInfo);
                }
            } catch (SQLException e) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                        Messages.getString("TdsqlDirectRefreshTopologyException.FailedToCreateStatement"),
                        e);
            }
        } catch (SQLException e) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectRefreshTopologyException.class,
                    Messages.getString("TdsqlDirectRefreshTopologyException.FailedToExecuteRefreshTopologySql",
                            new Object[]{TDSQL_DIRECT_REFRESH_TOPOLOGY_SQL}),
                    e);
        }
    }

    private Map<String, Integer> getArrayIndexMap(List<String> list) {
        Map<String, Integer> m = new HashMap<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            m.put(list.get(i), i);
        }
        return m;
    }
}
