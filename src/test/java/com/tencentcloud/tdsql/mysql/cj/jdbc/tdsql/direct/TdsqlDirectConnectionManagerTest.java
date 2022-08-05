package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJCommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlDirectLoadBalanceStrategyFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.NodeMsg;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.*;


class TdsqlDirectConnectionManagerTest {
    private TdsqlHostInfo currentTdsqlHostInfo;
    private boolean tdsqlDirectMasterCarryOptOfReadOnlyMode = false;
    private final int retriesAllDown = 2;
    private final ConcurrentHashMap<TdsqlHostInfo, List<JdbcConnection>> connectionHolder = new ConcurrentHashMap<>();
    private ThreadPoolExecutor recycler;
    protected static final String URLM1 = "jdbc:tdsql-mysql:direct://"
            + "9.135.135.186:3306,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=rw";
    protected static final String URLS1 = "jdbc:tdsql-mysql:direct://"
            + "9.30.1.178:4015,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=ro";
    protected static final String URLS2 = "jdbc:tdsql-mysql:direct://"
            + "9.30.1.178:4016,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=ro";
    protected static final String USER_M = "gyokumeixie";
    protected static final String PASS_M = "Mkhdb*8532XucF";
    protected static final String USER_S = "tdsqlsys_normal";
    protected static final String PASS_S = "gl%LDY^1&OKWkLWQP^7&";
    static TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlAtomicLongMap.create();
    @Test
    public void TestFailOver() throws SQLException {
        TdsqlDirectLoadBalanceStrategyFactory instance = TdsqlDirectLoadBalanceStrategyFactory.getInstance();
        TdsqlLoadBalanceStrategy lc = instance.getStrategyInstance("Lc");
        JdbcConnection newConnection = createNewConnection(lc , true);
        System.out.println(newConnection);
        System.out.println(currentTdsqlHostInfo.getHostPortPair());
        System.out.println(newConnection.getHostPortPair());
    }

    @Test
    @BeforeEach
    public void creatQueue(){
        DatabaseUrlContainer originUrl_M = new DatabaseUrlContainer() {
            @Override
            public String getDatabaseUrl() {
                return URLM1;
            }
        };
        HostInfo hostInfo_M = new HostInfo(originUrl_M, "9.135.135.186", 3306, USER_M, PASS_M, null);
        TdsqlHostInfo tdsqlHostInfo_M = new TdsqlHostInfo(hostInfo_M);
        scheduleQueue.put(tdsqlHostInfo_M, new NodeMsg(0L, false));
        DatabaseUrlContainer originUrl_S1 = new DatabaseUrlContainer() {
            @Override
            public String getDatabaseUrl() {
                return URLS1;
            }
        };
        HostInfo hostInfo_S1 = new HostInfo(originUrl_S1, "9.30.1.178", 4015, USER_S, PASS_S, null);
        TdsqlHostInfo tdsqlHostInfo_S1 = new TdsqlHostInfo(hostInfo_S1);
        scheduleQueue.put(tdsqlHostInfo_S1, new NodeMsg(2L, true));

        DatabaseUrlContainer originUrl_S2 = new DatabaseUrlContainer() {
            @Override
            public String getDatabaseUrl() {
                return URLS2;
            }
        };
        HostInfo hostInfo_S2 = new HostInfo(originUrl_S2, "9.30.1.178", 4016, USER_S, PASS_S, null);
        TdsqlHostInfo tdsqlHostInfo_S2 = new TdsqlHostInfo(hostInfo_S2);
        scheduleQueue.put(tdsqlHostInfo_S2, new NodeMsg(3L, false));
    }

    public synchronized JdbcConnection createNewConnection(TdsqlLoadBalanceStrategy balancer, boolean tdsqlDirectMasterCarryOptOfReadOnlyMode) throws SQLException {
        this.tdsqlDirectMasterCarryOptOfReadOnlyMode = tdsqlDirectMasterCarryOptOfReadOnlyMode;
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
        TdsqlDirectReadWriteMode readWriteMode = RO;

        JdbcConnection connection;
        if (RW.equals(readWriteMode)){
            connection = pickConnection(scheduleQueueMaster, balancer);
        }else{
            //先进行从库的故障转移
            connection = failover(scheduleQueue, scheduleQueueSlave, balancer, tdsqlHostInfoList);
            //是否有必要将scheduleQueue中调度不了的从库移除,又因为该库宕机，那么之前的连接实例也有必要删除！
            for (TdsqlHostInfo tdsqlHostInfo: tdsqlHostInfoList){
                if (!scheduleQueueSlave.containsKey(tdsqlHostInfo) && !scheduleQueueMaster.containsKey(tdsqlHostInfo)){
                    scheduleQueue.remove(tdsqlHostInfo);
                    connectionHolder.remove(tdsqlHostInfo);
                }
            }
            //此时connection为空，说明从库连接建立失败，并且如果允许主库承接只读流量，那么建立主库连接
            if (connection == null && tdsqlDirectMasterCarryOptOfReadOnlyMode){
                connection = pickConnection(scheduleQueueMaster, balancer);
            }
        }

        List<JdbcConnection> holderList = connectionHolder.getOrDefault(currentTdsqlHostInfo,
                new CopyOnWriteArrayList<>());
        holderList.add(connection);
        connectionHolder.put(currentTdsqlHostInfo, holderList);
        scheduleQueue.incrementAndGet(currentTdsqlHostInfo);
        return connection;
    }


    public JdbcConnection failover(TdsqlAtomicLongMap scheduleQueue, TdsqlAtomicLongMap scheduleQueueSlave,
                                   TdsqlLoadBalanceStrategy balancer, List<TdsqlHostInfo> tdsqlHostInfoList){
        JdbcConnection connection = null;
        boolean getConnection = false;
        //进行failover操作
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
                        attemps ++;
                    }
                    //在尝试attemps-1次之前，都认为从库并不是因为宕机导致无法建立连接，可能是因为网络等，当最后一次遍历调度队列还是无法建立连接的时候，
                    // 就认为从库确实宕机了，此时将该节点从原始调度队列删除！
//                    if (attemps == retriesAllDown){
//                        scheduleQueue.remove(currentTdsqlHostInfo);
//                    }
                }
            }
        }while (attemps < retriesAllDown && !getConnection);
        return connection;
    }

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

    JdbcConnection pickConnection(TdsqlAtomicLongMap scheduleQueue, TdsqlLoadBalanceStrategy balancer) throws SQLException {
        TdsqlHostInfo tdsqlHostInfo = balancer.choice(scheduleQueue);
        currentTdsqlHostInfo = tdsqlHostInfo;
        JdbcConnection connection = ConnectionImpl.getInstance(tdsqlHostInfo);
        return connection;
    }

    void refreshScheduleQueue(TdsqlAtomicLongMap scheduleQueue, TdsqlAtomicLongMap scheduleQueueMaster, TdsqlAtomicLongMap scheduleQueueSlave, TdsqlHostInfo tdsqlHostInfo) {
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
}