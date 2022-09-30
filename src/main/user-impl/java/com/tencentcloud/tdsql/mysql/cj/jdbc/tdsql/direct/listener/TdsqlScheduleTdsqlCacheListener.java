package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.listener;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetUtil;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectDataSourceCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.NodeMsg;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;

import java.beans.PropertyChangeEvent;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TdsqlScheduleTdsqlCacheListener extends AbstractTdsqlCacheListener {

    private final String ownerUuid;
    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue;
    private final String tdsqlReadWriteMode;
    private final ConnectionUrl connectionUrl;

    public TdsqlScheduleTdsqlCacheListener(String tdsqlReadWriteMode,
                                           TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue, ConnectionUrl connectionUrl, String ownerUuid) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
        this.scheduleQueue = scheduleQueue;
        this.connectionUrl = connectionUrl;
        this.ownerUuid = ownerUuid;
    }

    /**
     * 属性变化监测，在子类中进行加锁
     * @param evt
     */
    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        ReentrantReadWriteLock refreshLock = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(ownerUuid).getTopoServer().getRefreshLock();
        try {
            refreshLock.writeLock().lock();
            super.propertyChange(evt);
        }finally {
            refreshLock.writeLock().unlock();
        }
    }

    /**
     * 主库变化
     * 1. 如果当前模式是RW, 将新主库加到schedulingQueue
     * 2. 如果当前模式是RO, 将新主库加到schedulingQueue
     *
     * @param offLines 离线主库
     * @param onLines 新上线主库
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleMaster(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
        for (TdsqlDataSetInfo onLine : onLines) {
            TdsqlHostInfo tdsqlHostInfo = TdsqlDataSetUtil.convertDataSetInfo(onLine, connectionUrl);
            tdsqlHostInfo.setOwnerUuid(this.ownerUuid);
            // 如果里面没有
            if ((scheduleQueue.containsKey(tdsqlHostInfo) && !scheduleQueue.get(tdsqlHostInfo).getIsMaster()) || !scheduleQueue.containsKey(tdsqlHostInfo)) {
                scheduleQueue.remove(tdsqlHostInfo);
                scheduleQueue.put(tdsqlHostInfo, new NodeMsg(0L, true));
            }
        }
        for (TdsqlDataSetInfo offLine : offLines) {
            TdsqlHostInfo tdsqlHostInfo = TdsqlDataSetUtil.convertDataSetInfo(offLine, connectionUrl);
            if (scheduleQueue.containsKey(tdsqlHostInfo)) {
                scheduleQueue.remove(tdsqlHostInfo);
            }
        }
    }

    /**
     * 从库变化
     * 1. RW or RO模式都会将新从库加到schedulingQueue
     *
     * @param offLines 离线从库
     * @param onLines 新上线从库
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleSlave(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
        for (TdsqlDataSetInfo slave : onLines) {
            TdsqlHostInfo tdsqlHostInfo = TdsqlDataSetUtil.convertDataSetInfo(slave, connectionUrl);
            tdsqlHostInfo.setOwnerUuid(this.ownerUuid);
            if ((scheduleQueue.containsKey(tdsqlHostInfo) && scheduleQueue.get(tdsqlHostInfo).getIsMaster()) || !scheduleQueue.containsKey(tdsqlHostInfo)) {
                scheduleQueue.remove(tdsqlHostInfo);
                scheduleQueue.put(tdsqlHostInfo, new NodeMsg(0L, false));
            }
        }
        for (TdsqlDataSetInfo oldSlave : offLines) {
            TdsqlHostInfo tdsqlHostInfo = TdsqlDataSetUtil.convertDataSetInfo(oldSlave, connectionUrl);
            if (scheduleQueue.containsKey(tdsqlHostInfo) && !scheduleQueue.get(tdsqlHostInfo).getIsMaster()) {
                scheduleQueue.remove(tdsqlHostInfo);
            }
        }
    }
}
