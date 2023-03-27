package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.cluster;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logWarn;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.datasource.TdsqlDirectDataSourceCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlWaitUtil;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 所有数据节点缓存.
 */
public class TdsqlDataSetCache {

    private final String ownerUuid;
    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    private final List<TdsqlDataSetInfo> masters = new CopyOnWriteArrayList<>();
    private final List<TdsqlDataSetInfo> slaves = new CopyOnWriteArrayList<>();
    private boolean masterCached = false;
    private boolean slaveCached = false;

    public static final String MASTERS_PROPERTY_NAME = "masters";
    public static final String SLAVES_PROPERTY_NAME = "slaves";

    public TdsqlDataSetCache(String ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    /**
     * 等待第一次缓存完成.
     *
     * @param interval 重试间隔(秒)
     * @param count 重试次数
     * @return 是否第一次缓存完成
     */
    public boolean waitCached(int interval, int count) {
        try {
            TdsqlWaitUtil.waitFor(interval, count, this::isCached);
        } catch (InterruptedException e) {
            logError("[" + this.ownerUuid + "] Wait cached timeout, " + e.getMessage(), e);
        }
        return isCached();
    }


    /**
     * 添加属性变化的监听.
     *
     * @param listener the listener
     */
    public void addListener(PropertyChangeListener listener) {
        this.propertyChangeSupport.addPropertyChangeListener(listener);
    }

    public List<TdsqlDataSetInfo> getMasters() {
        ReentrantReadWriteLock refreshLock = TdsqlDirectDataSourceCounter.getInstance()
                .getTdsqlDirectInfo(this.ownerUuid).getTopoServer().getRefreshLock();
        refreshLock.readLock().lock();
        try {
            return masters;
        } finally {
            refreshLock.readLock().unlock();
        }
    }

    public void setMasters(List<TdsqlDataSetInfo> newMasters) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid)
                .getTopoServer();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            // 当获取到的主库拓扑信息为空的时候，需要分多种情况判断
            String tdsqlDirectReadWriteMode = topoServer.getTdsqlDirectReadWriteMode();
            if (newMasters.isEmpty()) {
                // 只读模式，不再需要执行更新缓存的代码逻辑
                if (TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode)) {
                    logWarn("[" + this.ownerUuid + "] After update, master is empty, but we in RO mode,"
                            + " so to be continue!");
                    if (!masterCached) {
                        masterCached = true;
                    }
                    return;
                } else if (TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(tdsqlDirectReadWriteMode)
                        && this.masters.isEmpty()) {
                    // 读写模式，且缓存的主库拓扑信息也为空，不再需要执行更新缓存的代码逻辑
                    logWarn("[" + this.ownerUuid + "] After update, master is empty, although we in RW mode,"
                            + " cached master also empty, so to be continue!");
                    if (!masterCached) {
                        masterCached = true;
                    }
                    return;
                }
            }

            if (!newMasters.equals(this.masters)) {
                logInfo("[" + this.ownerUuid + "] DataSet master have changed, old: " + this.masters + ", new: "
                        + newMasters);
                propertyChangeSupport.firePropertyChange(MASTERS_PROPERTY_NAME,
                        TdsqlDataSetUtil.copyDataSetList(this.masters),
                        TdsqlDataSetUtil.copyDataSetList(newMasters));
                this.masters.clear();
                this.masters.addAll(newMasters);
                if (!masterCached) {
                    masterCached = true;
                }
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }
    }

    public List<TdsqlDataSetInfo> getSlaves() {
        ReentrantReadWriteLock refreshLock = TdsqlDirectDataSourceCounter.getInstance()
                .getTdsqlDirectInfo(this.ownerUuid).getTopoServer().getRefreshLock();
        refreshLock.readLock().lock();
        try {
            return slaves;
        } finally {
            refreshLock.readLock().unlock();
        }
    }

    public void setSlaves(List<TdsqlDataSetInfo> newSlaves) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid)
                .getTopoServer();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            // 当获取到的从库拓扑信息为空的时候，需要分多种情况判断
            String tdsqlDirectReadWriteMode = topoServer.getTdsqlDirectReadWriteMode();
            if (newSlaves.isEmpty()) {
                // 读写模式，不再需要执行更新缓存的代码逻辑
                if (TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(tdsqlDirectReadWriteMode)) {
                    logWarn("[" + this.ownerUuid
                            + "] After update, slaves is empty, but we in RW mode, so to be continue!");
                    if (!slaveCached) {
                        slaveCached = true;
                    }
                    return;
                } else if (TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode)
                        && this.slaves.isEmpty()) {
                    // 只读模式，且缓存的从库拓扑信息也为空，不再需要执行更新缓存的代码逻辑
                    logWarn("[" + this.ownerUuid + "] After update, slaves is empty, although we in RO mode, "
                            + "cached slaves also empty, so to be continue!");
                    if (!slaveCached) {
                        slaveCached = true;
                    }
                    return;
                }
            }

            Integer tdsqlMaxSlaveDelay = topoServer.getTdsqlDirectMaxSlaveDelaySeconds();
            // 如果设置了从库最大延迟并且数据库实延迟大于这个设定的延迟
            if (tdsqlMaxSlaveDelay > 0) {
                newSlaves.removeIf(dsInfo -> dsInfo.getDelay() >= tdsqlMaxSlaveDelay);
            }
            // 主从复制断开时，防止连接到异常从库上
            newSlaves.removeIf(dsInfo -> dsInfo.getDelay() >= 100000);
            if (!newSlaves.equals(this.slaves)) {
                logInfo("[" + this.ownerUuid + "] DataSet slave have changed, old: " + this.slaves + ", new: "
                        + newSlaves);
                propertyChangeSupport.firePropertyChange(SLAVES_PROPERTY_NAME,
                        TdsqlDataSetUtil.copyDataSetList(this.slaves),
                        TdsqlDataSetUtil.copyDataSetList(newSlaves));
                this.slaves.clear();
                this.slaves.addAll(newSlaves);
                if (!slaveCached) {
                    slaveCached = true;
                }
                logInfo("[" + this.ownerUuid + "] After update, slaves is: " + this.slaves);
            }
            if (TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode) && newSlaves.isEmpty()
                    && this.slaves.isEmpty()) {
                logInfo("[" + this.ownerUuid + "] DataSet slave is null! but in ReadOnly mode, So NOOP!");
                if (!slaveCached) {
                    slaveCached = true;
                }
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }
    }

    public boolean isCached() {
        TdsqlDirectTopoServer topoServer = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid)
                .getTopoServer();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            if (masterCached && slaveCached) {
                return true;
            } else {
                return masterCached && TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(this.ownerUuid).
                        getTdsqlDirectConnectionManager().isTdsqlDirectMasterCarryOptOfReadOnlyMode();
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }

    }
}
