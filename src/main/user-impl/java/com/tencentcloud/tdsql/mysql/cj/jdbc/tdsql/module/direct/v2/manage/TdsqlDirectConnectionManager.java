package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum.DIRECT;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.AbstractTdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlInvalidConnectionPropertyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCreateConnectionException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.AbstractTdsqlCaughtRunnable;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlSynchronousExecutor;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>TDSQL专属，直连模式连接管理器类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConnectionManager {

    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final Map<String, List<JdbcConnection>> liveConnectionMap;
    private final Map<String, Long> slaveBlacklist;
    private final ReentrantLock lock;
    private final TdsqlLoadBalanceStrategy<TdsqlDirectConnectionCounter> algorithm;
    private final Executor netTimeoutExecutor;
    private ThreadPoolExecutor recycler;

    /**
     * 构造方法
     *
     * @param dataSourceConfig 数据源配置信息
     */
    public TdsqlDirectConnectionManager(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
        this.liveConnectionMap = new ConcurrentHashMap<>();
        this.slaveBlacklist = new HashMap<>();
        this.lock = new ReentrantLock();
        // 只读模式需要初始化负载均衡策略算法类实例
        if (RO.equals(dataSourceConfig.getTdsqlDirectReadWriteMode())) {
            this.algorithm = TdsqlLoadBalanceStrategyFactory.getInstance(
                    dataSourceConfig.getTdsqlLoadBalanceStrategy());
        } else {
            this.algorithm = null;
        }
        // 初始化关闭数据库连接SocketTimeout执行器
        this.netTimeoutExecutor = new TdsqlSynchronousExecutor(dataSourceConfig.getDataSourceUuid());
        initializeRecycler();
    }

    private void initializeRecycler() {
        this.recycler = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Recycler-pool-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());
        this.recycler.allowCoreThreadTimeOut(true);
    }

    /**
     * 创建新的数据库连接
     *
     * @return 新连接
     * @throws SQLException 当创建连接失败时抛出
     */
    public JdbcConnection createNewConnection() throws SQLException {
        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();
        switch (rwMode) {
            case RW:
                return this.createMasterConnection();
            case RO:
                return this.createSlaveConnection();
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                                new Object[]{rwMode.getRwModeName()}));
        }
    }

    public void asyncCloseAllConnection(TdsqlDirectHostInfo directHostInfo) {
        if (!DIRECT.equals(directHostInfo.getConnectionMode())) {
            return;
        }
        List<JdbcConnection> toCloseConnections = new ArrayList<>();
        toCloseConnections.addAll(this.liveConnectionMap.get(directHostInfo.getHostPortPair()));
        if (toCloseConnections == null || toCloseConnections.size() == 0) {
            return;
        }
        // 因为connection count在调用updateMaster、addSlave和removeSlave时候就已经更新了，
        // 所以这里不需要调用removeConnectionCount，也避免再次上锁
        // this.removeConnectionCount(directHostInfo);
        this.removeConnection(directHostInfo, null);
        this.recycler.submit(new AsyncCloseTask(toCloseConnections, this.netTimeoutExecutor, this.dataSourceConfig.getTdsqlDirectCloseConnTimeoutMillis()));
    }

    /**
     * 根据主机信息，关闭所有该主机的存量连接
     *
     * @param directHostInfo 直连模式专属主机信息
     */
    public void closeAllConnection(TdsqlDirectHostInfo directHostInfo) {
        if (DIRECT.equals(directHostInfo.getConnectionMode())) {
            for (Entry<String, List<JdbcConnection>> entry : this.liveConnectionMap.entrySet()) {
                if (entry != null && entry.getKey().equals(directHostInfo.getHostPortPair())) {
                    for (JdbcConnection liveConnection : entry.getValue()) {
                        try {
                            if (liveConnection != null && !liveConnection.isClosed()) {
                                liveConnection.setNetworkTimeout(this.netTimeoutExecutor,
                                        this.dataSourceConfig.getTdsqlDirectCloseConnTimeoutMillis());
                                liveConnection.close();
                            }
                        } catch (SQLException e) {
                            // Eat this exception.
                        } finally {
                            this.decrementConnection(directHostInfo);
                        }
                    }
                    this.removeConnection(directHostInfo, null);
                }
            }
        }
    }

    /**
     * 根据主机信息和连接实例，关闭该主机的这个连接
     *
     * @param directHostInfo 直连模式专属主机信息
     * @param jdbcConnection 已建立待关闭的数据库连接
     */
    public void closeConnection(TdsqlDirectHostInfo directHostInfo, JdbcConnection jdbcConnection) {
        if (DIRECT.equals(directHostInfo.getConnectionMode())) {
            try {
                if (jdbcConnection != null && !jdbcConnection.isClosed()) {
                    jdbcConnection.setNetworkTimeout(this.netTimeoutExecutor,
                            this.dataSourceConfig.getTdsqlDirectCloseConnTimeoutMillis());
                    jdbcConnection.close();
                }
            } catch (SQLException e) {
                // Eat this exception.
            } finally {
                this.decrementConnection(directHostInfo);
                this.removeConnection(directHostInfo, jdbcConnection);
            }
        }
    }

    /**
     * 获取所有存量连接
     *
     * @return 所有存量连接
     */
    public Map<String, List<JdbcConnection>> getLiveConnectionMap() {
        return Collections.unmodifiableMap(this.liveConnectionMap);
    }

    /**
     * 创建主库数据库连接
     *
     * @return 新连接
     * @throws SQLException 当创建连接失败时抛出
     */
    private JdbcConnection createMasterConnection() throws SQLException {
        TdsqlDirectScheduleServer scheduleServer = this.dataSourceConfig.getScheduleServer();

        ReentrantReadWriteLock.ReadLock readLock = scheduleServer.getSchedualeReadLock();
        try {
            readLock.lock();
            // 当没有主库调度信息时，记录日志并抛出异常
            TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();
            if (masterCounter == null || masterCounter.getTdsqlHostInfo() == null) {
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCreateConnectionException.class,
                        Messages.getString("TdsqlDirectConnectionManagerException.EmptySchedule", new Object[]{"MASTER"}));
            }
            // 创建数据库连接
            JdbcConnection jdbcConnection = ConnectionImpl.getInstance(masterCounter.getTdsqlHostInfo());
            // 累加计数器
            masterCounter.getCount().increment();
            // 保存已建立的数据库连接
            this.cacheConnection(masterCounter.getTdsqlHostInfo(), jdbcConnection);
            return jdbcConnection;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 创建备库数据库连接
     *
     * @return 新连接
     * @throws SQLException 当创建连接失败时抛出
     */
    private JdbcConnection createSlaveConnection() throws SQLException {
        TdsqlDirectScheduleServer scheduleServer = this.dataSourceConfig.getScheduleServer();

        ReentrantReadWriteLock.ReadLock readLock = scheduleServer.getSchedualeReadLock();
        try {
            readLock.lock();
            // 当没有备库调度信息时，记录日志并抛出异常
            Set<TdsqlDirectConnectionCounter> slaveSet = scheduleServer.getSlaveSet();
            if (slaveSet.isEmpty()) {
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCreateConnectionException.class,
                        Messages.getString("TdsqlDirectConnectionManagerException.EmptySchedule", new Object[]{"SLAVE"}));
            }

            // 从备库的调度信息中移除已经被加入到黑名单的备库信息
            Set<TdsqlDirectConnectionCounter> filteredSlaveSet = this.filterSlaveBlacklist(slaveSet);
            // 如果所有备库都已经被加入到了黑名单
            if (filteredSlaveSet.isEmpty()) {
                // 如果设置了主库承接只读流量开关，则建立到主库的数据库连接
                if (this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                    return createMasterConnection();
                }
                // 否则，记录日志并抛出异常
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCreateConnectionException.class,
                        Messages.getString("TdsqlDirectConnectionManagerException.AllSlaveInBlacklist"));
            }

            // 根据负载均衡策略算法，从允许调度的备库集合中，选取一个备库信息
            TdsqlConnectionCounter slaveCounter = this.algorithm.choice(Collections.unmodifiableSet(filteredSlaveSet));
            // 如果选择失败
            if (slaveCounter == null) {
                // 如果设置了主库承接只读流量开关，则建立到主库的数据库连接
                if (this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                    return createMasterConnection();
                }
                // 否则，记录日志并抛出异常
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid, TdsqlDirectCreateConnectionException.class,
                        Messages.getString("TdsqlDirectConnectionManagerException.AllSlaveIsZeroWeight"));
            }

            // 尝试建立到备库的数据库连接，最大尝试建立三次，否则加入黑名单
            int numRetries = 3;
            JdbcConnection jdbcConnection = null;
            for (int attempts = 1; attempts <= numRetries; ) {
                AbstractTdsqlHostInfo slaveHostInfo = slaveCounter.getTdsqlHostInfo();
                try {
                    jdbcConnection = ConnectionImpl.getInstance(slaveHostInfo);
                    // 设置连接只读时，间接验证了连接的有效性
                    jdbcConnection.setReadOnly(true);
                    break;
                } catch (Throwable t) {
                    // 建立连接失败，或者连接无效，减少连接计数器的值
                    slaveCounter.getCount().decrement();
                    // 补偿性的尝试关闭连接
                    if (jdbcConnection != null) {
                        try {
                            jdbcConnection.close();
                        } catch (SQLException ex) {
                            // Eat this exception.
                        }
                    }
                    // 达到最大尝试次数后，加入黑名单，抛出异常
                    if (attempts == numRetries) {
                        this.addSlaveBlacklist(slaveHostInfo.getHostPortPair());
                        throw t;
                    }
                    // 否则，记录失败日志，继续下次尝试
                    logError(this.dataSourceUuid,
                            Messages.getString("TdsqlDirectConnectionManagerException.FailedToCreateSlaveConnection",
                                    new Object[]{slaveHostInfo.getHostPortPair(), attempts}), t);
                    attempts++;
                }
            }
            // 保存已建立的数据库连接
            this.cacheConnection((TdsqlDirectHostInfo) slaveCounter.getTdsqlHostInfo(), jdbcConnection);
            return jdbcConnection;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 保存已建立的数据库连接
     *
     * @param directHostInfo 已建立的连接的直连模式专属主机信息
     * @param jdbcConnection 已建立的数据库连接
     */
    private void cacheConnection(TdsqlDirectHostInfo directHostInfo, JdbcConnection jdbcConnection) {
        this.lock.lock();
        try {
            if (!this.liveConnectionMap.containsKey(directHostInfo.getHostPortPair())) {
                this.liveConnectionMap.put(directHostInfo.getHostPortPair(), new ArrayList<>());
            }
            this.liveConnectionMap.get(directHostInfo.getHostPortPair()).add(jdbcConnection);
        } finally {
            this.lock.unlock();
        }
    }

    private void removeConnectionCount(TdsqlDirectHostInfo directHostInfo) {
        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();
        TdsqlDirectScheduleServer scheduleServer = this.dataSourceConfig.getScheduleServer();
        switch (rwMode) {
            case RW:
                TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();
                if (masterCounter.getTdsqlHostInfo().equals(directHostInfo)) {
                    masterCounter.getCount().reset();
                }
                break;
            case RO:
                for (TdsqlDirectConnectionCounter slaveCounter : scheduleServer.getSlaveSet()) {
                    if (slaveCounter.getTdsqlHostInfo().equals(directHostInfo)) {
                        slaveCounter.getCount().reset();
                        break;
                    }
                }
                break;
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                                new Object[]{rwMode.getRwModeName()}));
        }
    }

    /**
     * 根据主机信息，减少其对应的连接计数器的值
     *
     * @param directHostInfo 直连模式专属主机信息
     */
    private void decrementConnection(TdsqlDirectHostInfo directHostInfo) {
        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();
        TdsqlDirectScheduleServer scheduleServer = this.dataSourceConfig.getScheduleServer();
        switch (rwMode) {
            case RW:
                TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();
                if (masterCounter.getTdsqlHostInfo().equals(directHostInfo)) {
                    masterCounter.getCount().decrement();
                }
                break;
            case RO:
                for (TdsqlDirectConnectionCounter slaveCounter : scheduleServer.getSlaveSet()) {
                    if (slaveCounter.getTdsqlHostInfo().equals(directHostInfo)) {
                        slaveCounter.getCount().decrement();
                        break;
                    }
                }
                break;
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.logException(this.dataSourceUuid,
                        TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                                new Object[]{rwMode.getRwModeName()}));
        }
    }

    /**
     * 根据主机信息和连接实例，移除其对应的缓存
     * 当连接实例为 {@code null} 时，则移除该主机的全部缓存
     *
     * @param directHostInfo 直连模式专属主机信息
     * @param jdbcConnection 待移除的连接实例
     */
    private void removeConnection(TdsqlDirectHostInfo directHostInfo, JdbcConnection jdbcConnection) {
        this.lock.lock();
        try {
            List<JdbcConnection> connList = this.liveConnectionMap.getOrDefault(directHostInfo.getHostPortPair(), null);
            if (connList != null) {
                if (jdbcConnection == null) {
                    connList.clear();
                } else {
                    connList.removeIf(conn -> conn.equals(jdbcConnection));
                }
                if (connList.isEmpty()) {
                    this.liveConnectionMap.remove(directHostInfo.getHostPortPair());
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 过滤掉已经加入到黑名单且未超时的备库信息
     *
     * @param originalSlaveSet 待过滤的备库连接计数器集合
     * @return 过滤后的备库连接计数器集合
     */
    private synchronized Set<TdsqlDirectConnectionCounter> filterSlaveBlacklist(
            Set<TdsqlDirectConnectionCounter> originalSlaveSet) {
        // 如果黑名单为空，则返回原始集合
        if (this.slaveBlacklist.isEmpty()) {
            return originalSlaveSet;
        }

        Set<String> blacklistKeys = this.slaveBlacklist.keySet();
        Set<TdsqlDirectConnectionCounter> filteredSlaveSet = new LinkedHashSet<>();

        for (TdsqlDirectConnectionCounter slaveCounter : originalSlaveSet) {
            String hostPortPair = slaveCounter.getTdsqlHostInfo().getHostPortPair();
            // 如果备库信息在黑名单中存在，判断其是否超时，如果已超时则从黑名单中移除，否则继续判断其它的备库
            if (blacklistKeys.contains(hostPortPair)) {
                Long timeout = this.slaveBlacklist.get(hostPortPair);
                if (System.currentTimeMillis() < timeout) {
                    continue;
                } else {
                    this.slaveBlacklist.remove(hostPortPair);
                }
            }
            // 黑名单中不存在或者黑名单已超时，加入允许调度从库集合
            filteredSlaveSet.add(slaveCounter);
        }
        return filteredSlaveSet;
    }

    /**
     * 加入到黑名单的备库默认的超时时间为30秒
     *
     * @param hostPortPair 待加入黑名单的备库地址端口字符串
     */
    private synchronized void addSlaveBlacklist(String hostPortPair) {
        this.slaveBlacklist.put(hostPortPair, System.currentTimeMillis() + 30 * 1000);
    }

    private static class AsyncCloseTask extends AbstractTdsqlCaughtRunnable {
        private List<JdbcConnection> toCloseList;

        private Executor netTimeoutExecutor;

        private Integer closeConnTimeoutMillis;

        private AsyncCloseTask(List<JdbcConnection> toCloseList, Executor netTimeoutExecutor, Integer closeConnTimeoutMillis) {
            this.toCloseList = toCloseList;
            this.netTimeoutExecutor = netTimeoutExecutor;
            this.closeConnTimeoutMillis = closeConnTimeoutMillis;
        }

        @Override
        public void caughtAndRun() {
            for (JdbcConnection jdbcConnection : toCloseList) {
                try {
                    logInfo("check close connection! host: " + jdbcConnection.getHostPortPair());
                    if (jdbcConnection != null && !jdbcConnection.isClosed()) {
                        logInfo("start close connection! host: " + jdbcConnection.getHostPortPair());
                        jdbcConnection.setNetworkTimeout(this.netTimeoutExecutor, closeConnTimeoutMillis);
                        jdbcConnection.close();
                        logInfo("close connection successfully! host:" + jdbcConnection.getHostPortPair());
                    }
                } catch (SQLException e) {
                    // Eat this exception.
                    logError("close connection failed! host:" + jdbcConnection.getHostPortPair());
                }
            }
        }
    }
}
