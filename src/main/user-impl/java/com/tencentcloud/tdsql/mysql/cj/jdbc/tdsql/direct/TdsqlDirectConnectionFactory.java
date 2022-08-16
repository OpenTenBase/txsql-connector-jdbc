package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.convert;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.exception.TdsqlNoBackendInstanceException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlDirectLoadBalanceStrategyFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectConnectionFactory {

    public static boolean directMode = false;
    public static boolean allSlaveCrash = false;
    private TdsqlLoadBalanceStrategy balancer;
    private boolean tdsqlDirectMasterCarryOptOfReadOnlyMode = false;
    private static Map<TdsqlHostInfo, Long> globalBlocklist = new HashMap<>();

    private TdsqlDirectConnectionFactory() {
    }

    public JdbcConnection createConnection(ConnectionUrl connectionUrl) throws SQLException {
        directMode = true;
        Properties props = connectionUrl.getConnectionArgumentsAsProperties();
        String tdsqlDirectMasterCarryOptOfReadOnlyModeStr = props.getProperty(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName(), "false");
        try {
            tdsqlDirectMasterCarryOptOfReadOnlyMode = Boolean.parseBoolean(tdsqlDirectMasterCarryOptOfReadOnlyModeStr);
        } catch (Exception e) {
            String errMessage = Messages.getString("ConnectionProperties.badValurForTdsqlDirectMasterCarryOptOfReadOnlyMode",
                    new Object[]{tdsqlDirectMasterCarryOptOfReadOnlyModeStr}) +
                    Messages.getString("ConnectionProperties.tdsqlDirectMasterCarryOptOfReadOnlyMode");
            throw SQLError.createSQLException(errMessage,
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        ReentrantReadWriteLock refreshLock = topoServer.getRefreshLock();
        topoServer.initialize(connectionUrl);
        TdsqlDirectReadWriteMode readWriteMode = convert(topoServer.getTdsqlDirectReadWriteMode());
        List<TdsqlDataSetInfo> masters = TdsqlDataSetCache.getInstance().getMasters();
        List<TdsqlDataSetInfo> slaves = TdsqlDataSetCache.getInstance().getSlaves();

        if (RW.equals(readWriteMode) && masters.isEmpty()) {
            throw new TdsqlNoBackendInstanceException("No master instance found, master size: 0");
        }
        if (RO.equals(readWriteMode) && slaves.isEmpty()) {
            if (tdsqlDirectMasterCarryOptOfReadOnlyMode){
                if (masters.isEmpty()){
                    throw new TdsqlNoBackendInstanceException("In ReadOnly mode, No slave and master instance found");
                }
            }else {
                throw new TdsqlNoBackendInstanceException("No slave instance found");
            }

        }
        TdsqlDirectLoggerFactory.logDebug(
                "New create connection request received, now master: " + masters + ", now slaves: " + slaves);

        String strategy = props.getProperty(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName(), "Sed");


        refreshLock.readLock().lock();
        this.balancer = TdsqlDirectLoadBalanceStrategyFactory.getInstance().getStrategyInstance(strategy);

        JdbcConnection newConnection;
        //此时已经得到了负载均衡实例
        try {
            newConnection = TdsqlDirectConnectionManager.getInstance()
                    .createNewConnection(this.balancer, tdsqlDirectMasterCarryOptOfReadOnlyMode);
        } finally {
            refreshLock.readLock().unlock();
        }
        return newConnection;
    }

    public void closeConnection(JdbcConnection jdbcConnection, HostInfo hostInfo) {
        TdsqlHostInfo tdsqlHostInfo = new TdsqlHostInfo(hostInfo);
        TdsqlDirectConnectionManager.getInstance().getConnectionList(tdsqlHostInfo)
                .removeIf(cachedConnection -> cachedConnection.equals(jdbcConnection));
        TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
        if (scheduleQueue.containsKey(tdsqlHostInfo)) {
            scheduleQueue.decrementAndGet(tdsqlHostInfo);
        }
    }
    public synchronized static Map<TdsqlHostInfo, Long> getGlobalBlocklist(){
        Set<TdsqlHostInfo> keys = globalBlocklist.keySet();
        Iterator i = keys.iterator();
        //根据超时来判断是否移出阻塞队列，当超时时间<当前时间，则表示进行检测
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        while(i.hasNext()) {
            TdsqlHostInfo tdsqlHostInfo = (TdsqlHostInfo)i.next();
            Long timeout = globalBlocklist.get(tdsqlHostInfo);
            //在一段时间之后，进行宕机节点的移除移除条件是节点存活并且延迟小于url中设定的延迟
            if (timeout != null && timeout < System.currentTimeMillis()
                    && tdsqlHostInfo.getDelay() < topoServer.getTdsqlDirectMaxSlaveDelaySeconds() && tdsqlHostInfo.getAlive()) {
                //如果存活，并且延迟小于设定的延迟
                synchronized(globalBlocklist) {
                    i.remove();
                }
            }
        }
        return globalBlocklist;
    }

    public static TdsqlDirectConnectionFactory getInstance() {
        return TdsqlDirectConnectionFactory.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlDirectConnectionFactory INSTANCE = new TdsqlDirectConnectionFactory();
    }


}
