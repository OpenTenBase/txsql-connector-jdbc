package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.exceptions.CJCommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.convert;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public final class TdsqlDirectConnectionManager {

    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();
    private ThreadPoolExecutor recycler;
    private TdsqlHostInfo currentTdsqlHostInfo;
    private final int retriesAllDown = 10;
    private boolean tdsqlDirectMasterCarryOptOfReadOnlyMode = false;

    private TdsqlDirectConnectionManager() {
        initializeCompensator();
        initializeRecycler();
    }

    public static TdsqlDirectConnectionManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public synchronized JdbcConnection createNewConnection(TdsqlLoadBalanceStrategy balancer, boolean tdsqlDirectMasterCarryOptOfReadOnlyMode) throws SQLException {
        this.tdsqlDirectMasterCarryOptOfReadOnlyMode = tdsqlDirectMasterCarryOptOfReadOnlyMode;
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = topoServer.getScheduleQueue();
        //此时scheduleQueue中不仅有主库还有从库,此时将主从库分开
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueueSlave = TdsqlAtomicLongMap.create();
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueueMaster = TdsqlAtomicLongMap.create();
        List<TdsqlHostInfo> tdsqlHostInfoList = Collections.unmodifiableList(
                new ArrayList<>(scheduleQueue.asMap().keySet()));
        //根据调度队列中节点的isMaster字段进行主从区分
        for (TdsqlHostInfo tdsqlHostInfo: tdsqlHostInfoList){
            if (scheduleQueue.get(tdsqlHostInfo).getIsMaster()){
                scheduleQueueMaster.put(tdsqlHostInfo, scheduleQueue.get(tdsqlHostInfo));
            }else{
                scheduleQueueSlave.put(tdsqlHostInfo, scheduleQueue.get(tdsqlHostInfo));
            }
        }
        TdsqlDirectReadWriteMode readWriteMode = convert(topoServer.getTdsqlDirectReadWriteMode());

        JdbcConnection connection;
        if (RW.equals(readWriteMode)){
             connection = pickConnection(scheduleQueueMaster, balancer);
        }else{
            //先进行从库的故障转移
            connection = failover(scheduleQueue, scheduleQueueSlave, balancer, tdsqlHostInfoList);
            //是否有必要将scheduleQueue中调度不了的从库移除
            for (TdsqlHostInfo tdsqlHostInfo: tdsqlHostInfoList){
                //如果slave中没有，并且master中也没有该节点，那么就说明该从节点调度失败将其从原始调度队列中删除！
                if (!scheduleQueueSlave.containsKey(tdsqlHostInfo) && !scheduleQueueMaster.containsKey(tdsqlHostInfo)){
                    scheduleQueue.remove(tdsqlHostInfo);
                    //既然节点宕机了，那么保存节点连接实例的map中的信息也要删除！
                    connectionHolder.remove(tdsqlHostInfo);
                }
            }
            //此时connection为空，说明从库连接建立失败，并且如果允许主库承接只读流量，那么建立主库连接
            if (connection == null){
                if (tdsqlDirectMasterCarryOptOfReadOnlyMode){
                    connection = pickConnection(scheduleQueueMaster, balancer);
                } else {
                    throw new SQLException("there is no slave available");
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

    /**
     * 从库进行故障转移
     * @param scheduleQueue         所有节点的原始调度队列
     * @param scheduleQueueSlave    从库调度队列
     * @param balancer              负载均衡策略
     * @param tdsqlHostInfoList     所有节点的KeySet
     * @return JdbcConnection
     */
    public JdbcConnection failover(TdsqlAtomicLongMap scheduleQueue, TdsqlAtomicLongMap scheduleQueueSlave,
                                   TdsqlLoadBalanceStrategy balancer, List<TdsqlHostInfo> tdsqlHostInfoList){
        JdbcConnection connection = null;
        boolean getConnection = false;
        //进行failover操作, attemps 参数代表最多循环遍历scheduleQueueSlave的次数
        int attemps = 0;
        do {
            //因为在调度失败之后，会将节点从调度队列中删除，所以在每一次列表调度全部失败之后、下一次列表调度之前，要将列表中的节点恢复
            if (scheduleQueueSlave.isEmpty()){
                for (TdsqlHostInfo tdsqlHostInfo: tdsqlHostInfoList){
                    if (!scheduleQueue.get(tdsqlHostInfo).getIsMaster()){
                        scheduleQueueSlave.put(tdsqlHostInfo, scheduleQueue.get(tdsqlHostInfo));
                    }
                }
            }
            //此步骤将从库尝试一遍，
            try {
                connection = pickConnection(scheduleQueueSlave, balancer);
                TdsqlLoggerFactory.logInfo("Create connection success [" + currentTdsqlHostInfo.getHostPortPair() + "], return it.");
                getConnection = true;
            }catch (SQLException e){
                if (shouldExceptionTriggerConnectionSwitch(e)){
                    //此步骤需要进行异常处理，即从库连接建立失败之后，需要将该从库从调度队列中移除,从库全部失败调度主库
                    TdsqlLoggerFactory.logError(
                            "Could not create connection to database server [" + currentTdsqlHostInfo.getHostPortPair()
                                    + "], starting to schedule other nodes.", e);
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
        }while (attemps < retriesAllDown && !getConnection);
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
     * 该方法进行正常的连接建立
     * @param scheduleQueue
     * @param balancer
     * @return
     * @throws SQLException
     */
    public JdbcConnection pickConnection(TdsqlAtomicLongMap scheduleQueue, TdsqlLoadBalanceStrategy balancer) throws SQLException {
        TdsqlHostInfo tdsqlHostInfo = balancer.choice(scheduleQueue);
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
        this.recycler.submit(new RecyclerTask(toCloseList));
    }

    private void initializeCompensator() {
        ScheduledThreadPoolExecutor compensator = new ScheduledThreadPoolExecutor(1,
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Compensator-pool-").build());
        compensator.scheduleWithFixedDelay(new CompensatorTask(), 0L, 1L, TimeUnit.SECONDS);
    }

    private void initializeRecycler() {
        this.recycler = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Recycler-pool-").build(),
                new AbortPolicy());
        this.recycler.allowCoreThreadTimeOut(true);
    }

    private static class CompensatorTask extends AbstractTdsqlCaughtRunnable {

        @Override
        public void caughtAndRun() {
            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = TdsqlDirectConnectionManager.getInstance()
                    .getAllConnection();
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

        private final List<String> recycleList;
        private final Executor netTimeoutExecutor = new TdsqlSynchronousExecutor();

        private RecyclerTask(List<String> recycleList) {
            this.recycleList = recycleList;
        }

        @Override
        public void caughtAndRun() {
            ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> allConnection =
                    TdsqlDirectConnectionManager.getInstance().getAllConnection();
            Iterator<Entry<TdsqlHostInfo, List<JdbcConnection>>> entryIterator = allConnection.entrySet()
                    .iterator();
            while (entryIterator.hasNext()) {
                Entry<TdsqlHostInfo, List<JdbcConnection>> entry = entryIterator.next();
                String holdHostPortPair = entry.getKey().getHostPortPair();
                if (recycleList.contains(holdHostPortPair)) {
                    TdsqlDirectLoggerFactory.logDebug("Start close [" + holdHostPortPair + "]'s connections!");
                    for (JdbcConnection jdbcConnection : entry.getValue()) {
                        ConnectionImpl connection = (ConnectionImpl) jdbcConnection;
                        if (connection != null && !connection.isClosed()) {
                            try {
                                connection.setNetworkTimeout(netTimeoutExecutor, TdsqlDirectTopoServer.getInstance()
                                        .getTdsqlDirectCloseConnTimeoutMillis());
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

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionManager INSTANCE = new TdsqlDirectConnectionManager();
    }
}
