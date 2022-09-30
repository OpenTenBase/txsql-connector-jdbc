package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJCommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.exception.TdsqlNoBackendInstanceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectDataSourceCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlDirectLoadBalanceStrategyFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.*;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.*;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private String ownerUuid;
    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();
    private ThreadPoolExecutor recycler;
    private TdsqlHostInfo currentTdsqlHostInfo;
    private TdsqlLoadBalanceStrategy balancer;
    private final int retriesAllDown = 5;
    private boolean tdsqlDirectMasterCarryOptOfReadOnlyMode = false;
    private boolean allSlaveCrash = false;

    public TdsqlDirectConnectionManager(String ownerUuid) {
        this.ownerUuid = ownerUuid;
        initializeCompensator(ownerUuid);
        initializeRecycler();
    }

    public synchronized JdbcConnection createNewConnection(ConnectionUrl connectionUrl) throws SQLException {

        Properties props = connectionUrl.getConnectionArgumentsAsProperties();
        String tdsqlDirectMasterCarryOptOfReadOnlyModeStr = props.getProperty(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName(), "false");
        try {
            this.tdsqlDirectMasterCarryOptOfReadOnlyMode = Boolean.parseBoolean(tdsqlDirectMasterCarryOptOfReadOnlyModeStr);
        } catch (Exception e) {
            String errMessage = Messages.getString("ConnectionProperties.badValurForTdsqlDirectMasterCarryOptOfReadOnlyMode",
                    new Object[]{tdsqlDirectMasterCarryOptOfReadOnlyModeStr}) +
                    Messages.getString("ConnectionProperties.tdsqlDirectMasterCarryOptOfReadOnlyMode");
            throw SQLError.createSQLException(errMessage,
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        String strategy = props.getProperty(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName(), "sed");
        this.balancer = TdsqlDirectLoadBalanceStrategyFactory.getInstance().getStrategyInstance(strategy);

        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getTopoServer();
        TdsqlDirectReadWriteMode readWriteMode = convert(topoServer.getTdsqlDirectReadWriteMode());

        List<TdsqlDataSetInfo> masters = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getDataSetCache().getMasters();
        List<TdsqlDataSetInfo> slaves = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getDataSetCache().getSlaves();
        if (RW.equals(readWriteMode) && masters.isEmpty()) {
            throw new TdsqlNoBackendInstanceException("No master instance found, master size: 0");
        }
        if (RO.equals(readWriteMode) && slaves.isEmpty()) {
            if (this.tdsqlDirectMasterCarryOptOfReadOnlyMode){
                if (masters.isEmpty()){
                    throw new TdsqlNoBackendInstanceException("In ReadOnly mode, No slave and master instance found");
                }
            }else {
                throw new TdsqlNoBackendInstanceException("No slave instance found");
            }
        }
        TdsqlDirectLoggerFactory.logDebug(
                "New create connection request received, now master: " + masters + ", now slaves: " + slaves);

        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = topoServer.getScheduleQueue();
        //此时scheduleQueue中不仅有主库还有从库,此时将主从库分开
        Map<TdsqlHostInfo, NodeMsg> scheduleQueueTemp = scheduleQueue.asMap();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueueSlave = TdsqlAtomicLongMap.create();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueueMaster = TdsqlAtomicLongMap.create();
//        List<TdsqlHostInfo> tdsqlHostInfoList = Collections.unmodifiableList(
//                new ArrayList<>(scheduleQueue.asMap().keySet()));
        List<TdsqlHostInfo> tdsqlHostInfoList = Collections.unmodifiableList(
                new ArrayList<>(scheduleQueueTemp.keySet()));
        //主从节点分离到对应的调度队列中
        for (TdsqlHostInfo tdsqlHostInfo : tdsqlHostInfoList){
            NodeMsg nodeMsg = scheduleQueueTemp.get(tdsqlHostInfo);
            if (nodeMsg.getIsMaster()){
                scheduleQueueMaster.put(tdsqlHostInfo, nodeMsg);
            }else {
                scheduleQueueSlave.put(tdsqlHostInfo, nodeMsg);
            }
        }

        JdbcConnection connection;
        //在读写模式或者主库可承接只读流量并且从库全部宕机，直接建立连接到主库
        if (RW.equals(readWriteMode) || (this.isAllSlaveCrash() && this.tdsqlDirectMasterCarryOptOfReadOnlyMode)){
             connection = pickConnection(scheduleQueueMaster, balancer);
            System.out.println("seimin-------------------------------scheduleQueue" + scheduleQueue.size()   + "--------------scheduleQueueMaster" + scheduleQueueMaster.size());
//             connection = pickConnection(scheduleQueue, balancer);
        }else{
            //先进行从库的故障转移
            connection = failover(scheduleQueue, scheduleQueueSlave, balancer);
            //如果slave中没有，并且master中也没有该节点，那么就说明该从节点调度失败将其从原始调度队列中删除！
            for (TdsqlHostInfo tdsqlHostInfo: tdsqlHostInfoList){
                if (!scheduleQueueSlave.containsKey(tdsqlHostInfo) && !scheduleQueueMaster.containsKey(tdsqlHostInfo) && scheduleQueue.containsKey(tdsqlHostInfo)){
                    scheduleQueue.remove(tdsqlHostInfo);
                    //既然节点宕机了。那么保存节点连接实例的map中的信息也要删除！
                    connectionHolder.remove(tdsqlHostInfo);
                }
            }
            //此时connection为空，说明从库连接建立失败，并且如果允许主库承接只读流量，那么建立主库连接
            if (connection == null){
                if (tdsqlDirectMasterCarryOptOfReadOnlyMode){
                    this.setAllSlaveCrash(true);
                    connection = pickConnection(scheduleQueueMaster, balancer);
                } else {
                    throw new SQLException("There is no slave available");
                }
            }
        }

        List<JdbcConnection> holderList = connectionHolder.getOrDefault(currentTdsqlHostInfo,
                new CopyOnWriteArrayList<>());
        holderList.add(connection);
        connectionHolder.put(currentTdsqlHostInfo, holderList);
        scheduleQueue.incrementAndGet(currentTdsqlHostInfo);
        return connection;
    }

    public boolean isAllSlaveCrash() {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getTopoServer();
        topoServer.getRefreshLock().readLock().lock();
        try {
            return this.allSlaveCrash;
        }finally {
            topoServer.getRefreshLock().readLock().unlock();
        }
    }

    public void setAllSlaveCrash(boolean allSlaveCrash) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getTopoServer();
        topoServer.getRefreshLock().readLock().lock();
        try {
            this.allSlaveCrash = allSlaveCrash;
        }finally {
            topoServer.getRefreshLock().readLock().unlock();
        }

    }

    public boolean isTdsqlDirectMasterCarryOptOfReadOnlyMode() {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).getTopoServer();
        topoServer.getRefreshLock().readLock().lock();
        try {
            return this.tdsqlDirectMasterCarryOptOfReadOnlyMode;
        }finally {
            topoServer.getRefreshLock().readLock().unlock();
        }
    }

    /**
     * 从库进行故障转移
     * @param scheduleQueue         所有节点的原始调度队列
     * @param scheduleQueueSlave    从库调度队列
     * @param balancer              负载均衡策略
     * @return JdbcConnection
     */
    public JdbcConnection failover(TdsqlAtomicLongMap scheduleQueue, TdsqlAtomicLongMap scheduleQueueSlave,
                                   TdsqlLoadBalanceStrategy balancer){
        if (scheduleQueueSlave.isEmpty()){
            return null;
        }
        JdbcConnection connection = null;
        boolean getConnection = false;
        //进行failover操作, attemps 参数代表最多循环遍历scheduleQueueSlave的次数
        int attemps = 0;
        List<TdsqlHostInfo> scheduleQueueSlaveKeys = Collections.unmodifiableList(
                new ArrayList<>(scheduleQueueSlave.asMap().keySet()));
        do {
            //因为在调度失败之后，会将节点从调度队列中删除，所以在每一次列表调度全部失败之后、下一次列表调度之前，要将列表中的节点恢复
            if (scheduleQueueSlave.isEmpty()){
                for (TdsqlHostInfo tdsqlHostInfo: scheduleQueueSlaveKeys){
                    //阻塞队列中的节点中的节点不再调度，所以此时如果节点在阻塞队列那么就不参与调度。
                    scheduleQueueSlave.put(tdsqlHostInfo, scheduleQueue.get(tdsqlHostInfo));
                }
            }
            //此步骤将从库尝试一遍
            try {
                connection = pickConnection(scheduleQueueSlave, balancer);
                if (connection != null){
                    TdsqlLoggerFactory.logInfo("Create connection success [" + currentTdsqlHostInfo.getHostPortPair() + "], return it.");
                    getConnection = true;
                }else{
                    //此时为空，说明pickConnection返回的值是空，因为在函数入口就判断了scheduleQueueSlave，所以此时scheduleQueueSlave不为空，但是选不到节点，
                    //那就说明在使用sed算法的时候，所有权重为0节点无法被调度
                    attemps ++;
                }
            }catch (SQLException e){
                if (shouldExceptionTriggerConnectionSwitch(e))
                {
                    //此步骤需要进行异常处理，即从库连接建立失败之后，需要将该从库从调度队列中移除,从库全部失败调度主库
                    TdsqlLoggerFactory.logError(
                            "Could not create connection to database server [" + currentTdsqlHostInfo.getHostPortPair()
                                    + "], starting to schedule other nodes.", e);
                    //
                    refreshScheduleQueue(null, null, scheduleQueueSlave, currentTdsqlHostInfo);
                    //当从库为空的时候，表明从库已经调度了一遍并且全部失败，那么此时将attemps+1，
                    if (scheduleQueueSlave.isEmpty()){
                        try {
                            Thread.sleep(250);
                        } catch (InterruptedException ie) {
                        }
                        attemps ++;
                    }
                }
            }
        }while (attemps < this.retriesAllDown && !getConnection);
        return connection;
    }
    /**
     * Local implementation for the connection switch exception checker.
     * @param t
     * @return
     */
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        String sqlState = null;
        if (t instanceof CommunicationsException || t instanceof CJCommunicationsException) {
            return true;
        } else if (t instanceof SQLException) {
            sqlState = ((SQLException) t).getSQLState();
        } else if (t instanceof CJException) {
            sqlState = ((CJException) t).getSQLState();
        }

        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                // connection error
                return true;
            }
        }
        return false;
    }
    /**
     * 该方法进行正常的连接建立，当使用sed并且节权值为0，那么就会选不到节点！
     * @param scheduleQueue
     * @param balancer
     * @return
     * @throws SQLException
     */
    public JdbcConnection pickConnection(TdsqlAtomicLongMap scheduleQueue, TdsqlLoadBalanceStrategy balancer) throws SQLException {
        TdsqlHostInfo tdsqlHostInfo = balancer.choice(scheduleQueue);
        if (tdsqlHostInfo == null){
            String errMessage = "Could not create connection to database server. Because no hosts in scheduleQueue";
            TdsqlLoggerFactory.logFatal(errMessage);
            throw SQLError.createSQLException(errMessage,
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                    null);
        }
        currentTdsqlHostInfo = tdsqlHostInfo;
        JdbcConnection connection = ConnectionImpl.getInstance(tdsqlHostInfo);
        return connection;
    }

    /**
     * 从调度队列中删除调度失败的节点
     * @param scheduleQueue
     * @param scheduleQueueMaster
     * @param scheduleQueueSlave
     * @param tdsqlHostInfo
     */
    public void refreshScheduleQueue(TdsqlAtomicLongMap scheduleQueue, TdsqlAtomicLongMap scheduleQueueMaster,
                                     TdsqlAtomicLongMap scheduleQueueSlave, TdsqlHostInfo tdsqlHostInfo){
        if (scheduleQueue != null && scheduleQueue.containsKey(tdsqlHostInfo)){
            scheduleQueue.remove(tdsqlHostInfo);
        }
        if (scheduleQueueSlave != null && scheduleQueueSlave.containsKey(tdsqlHostInfo)){
            scheduleQueueSlave.remove(tdsqlHostInfo);
        }
        if (scheduleQueueMaster != null && scheduleQueueMaster.containsKey(tdsqlHostInfo)){
            scheduleQueue.remove(tdsqlHostInfo);
        }
    }

    public ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> getAllConnection() {
        return connectionHolder;
    }

    public synchronized void close(List<String> toCloseList) {
        if (toCloseList == null || toCloseList.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("To close list is empty, close operation ignore!");
            return;
        }
        this.recycler.submit(new RecyclerTask(this.ownerUuid, toCloseList));
    }

    private void initializeCompensator(String ownerUuid) {
        ScheduledThreadPoolExecutor compensator = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Compensator-pool-").build());
        compensator.scheduleWithFixedDelay(new CompensatorTask(ownerUuid), 0L, 1L, TimeUnit.SECONDS);
    }

    private void initializeRecycler() {
        this.recycler = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Recycler-pool-").build(),
                new AbortPolicy());
        this.recycler.allowCoreThreadTimeOut(true);
    }

    private static class CompensatorTask extends AbstractTdsqlCaughtRunnable {
        private String ownerUuid;
        public CompensatorTask(String ownerUuid){
            this.ownerUuid = ownerUuid;
        }
        @Override
        public void caughtAndRun() {
            TdsqlDirectInfo tdsqlDirectInfo = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid);
            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = tdsqlDirectInfo.getTopoServer().getScheduleQueue();
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = tdsqlDirectInfo.getTdsqlDirectConnectionManager().getAllConnection();
            for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : connectionHolder.entrySet()) {
                TdsqlHostInfo tdsqlHostInfo = entry.getKey();
                int realCount = entry.getValue().size();
                if (!scheduleQueue.containsKey(tdsqlHostInfo)) {
                    return;
                }
                long currentCount = scheduleQueue.get(tdsqlHostInfo).getCount();
                //此处为了统一已经创建实例的数量与调度队列中节点被调度的次数。(后续看看逻辑问题！)
                if (realCount != currentCount) {
                    NodeMsg nodeMsg = scheduleQueue.get(tdsqlHostInfo);
                    nodeMsg.setCount((long) realCount);
                    scheduleQueue.put(tdsqlHostInfo, nodeMsg);
                }
            }
        }
    }

    private static class RecyclerTask extends AbstractTdsqlCaughtRunnable {

        private String ownerUuid;
        private final List<String> recycleList;
        private final Executor netTimeoutExecutor = new TdsqlSynchronousExecutor();

        private RecyclerTask(String ownerUuid, List<String> recycleList) {
            this.ownerUuid = ownerUuid;
            this.recycleList = recycleList;
        }

        @Override
        public void caughtAndRun() {
            TdsqlDirectInfo tdsqlDirectInfo = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid);
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> allConnection =
                    tdsqlDirectInfo.getTdsqlDirectConnectionManager().getAllConnection();
            Iterator<Entry<TdsqlHostInfo, List<JdbcConnection>>> entryIterator = allConnection.entrySet()
                    .iterator();
            while (entryIterator.hasNext()) {
                Entry<TdsqlHostInfo, List<JdbcConnection>> entry = entryIterator.next();
                String holdHostPortPair = entry.getKey().getHostPortPair();
                if (this.recycleList.contains(holdHostPortPair)) {
                    TdsqlDirectLoggerFactory.logDebug("Start close [" + holdHostPortPair + "]'s connections!");
                    for (JdbcConnection jdbcConnection : entry.getValue()) {
                        ConnectionImpl connection = (ConnectionImpl) jdbcConnection;
                        if (connection != null && !connection.isClosed()) {
                            try {
                                connection.setNetworkTimeout(this.netTimeoutExecutor,
                                        tdsqlDirectInfo.getTopoServer().getTdsqlDirectCloseConnTimeoutMillis());
                            } catch (Exception e) {
                                // ignore
                            } finally {
                                try {
                                    connection.close();
                                } catch (Exception e) {
                                    TdsqlDirectLoggerFactory.logError(
                                            "Closing [" + holdHostPortPair + "] connection failed!");
                                }
                            }
                        }
                    }
                    entryIterator.remove();
                    TdsqlDirectLoggerFactory.logDebug("Finish close [" + holdHostPortPair + "]'s connections!");
                } else {
                    TdsqlDirectLoggerFactory.logDebug("To closes not in connection holder! NOOP!");
                }
            }
        }
    }

    public List<JdbcConnection> getConnectionList(TdsqlHostInfo tdsqlHostInfo) {
        return connectionHolder.getOrDefault(tdsqlHostInfo, new CopyOnWriteArrayList<>());
    }

}
