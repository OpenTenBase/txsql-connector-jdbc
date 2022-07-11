package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RW;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlWaitUtil;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 所有数据节点缓存.
 */
public class TdsqlDataSetCache {

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    private final List<TdsqlDataSetInfo> masters = new CopyOnWriteArrayList<>();
    private final List<TdsqlDataSetInfo> slaves = new CopyOnWriteArrayList<>();
    private boolean masterCached = false;
    private boolean slaveCached = false;

    public static final String MASTERS_PROPERTY_NAME = "masters";
    public static final String SLAVES_PROPERTY_NAME = "slaves";

    private TdsqlDataSetCache() {
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
            TdsqlDirectLoggerFactory.logError("Wait cached timeout, " + e.getMessage(), e);
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

    public synchronized List<TdsqlDataSetInfo> getMasters() {
        TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().lock();
        try {
            return masters;
        } finally {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().unlock();
        }
    }

    public synchronized void setMasters(List<TdsqlDataSetInfo> newMasters) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            // 只有在RO模式下，主库的拓扑信息允许为空
            if (newMasters.isEmpty() && TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(
                    topoServer.getTdsqlDirectReadWriteMode())) {
                if (!masterCached) {
                    masterCached = true;
                }
                TdsqlDirectLoggerFactory.logDebug(
                        "After update, master is null, but we in RO mode, so to be continue!");
                return;
            }
            if (!newMasters.equals(this.masters)) {
                TdsqlDirectLoggerFactory.logDebug(
                        "DataSet master have changed, old: " + this.masters + ", new: " + newMasters);
                propertyChangeSupport.firePropertyChange(MASTERS_PROPERTY_NAME,
                        TdsqlDataSetUtil.copyDataSetList(this.masters),
                        TdsqlDataSetUtil.copyDataSetList(newMasters));
                this.masters.clear();
                this.masters.addAll(newMasters);
                if (!masterCached) {
                    masterCached = true;
                }
                TdsqlDirectLoggerFactory.logDebug("After update, master is: " + this.masters);
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }
    }

    public synchronized List<TdsqlDataSetInfo> getSlaves() {
        TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().lock();
        try {
            return slaves;
        } finally {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().unlock();
        }
    }

    public synchronized void setSlaves(List<TdsqlDataSetInfo> newSlaves) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            if (newSlaves.isEmpty() && TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(
                    topoServer.getTdsqlDirectReadWriteMode())) {
                if (!slaveCached) {
                    slaveCached = true;
                }
                TdsqlDirectLoggerFactory.logDebug(
                        "After update, slaves is null, but we in RW mode, so to be continue!");
                return;
            }
            Integer tdsqlMaxSlaveDelay = topoServer.getTdsqlDirectMaxSlaveDelaySeconds();
            if (tdsqlMaxSlaveDelay > 0) {
                newSlaves.removeIf(dsInfo -> dsInfo.getDelay() >= tdsqlMaxSlaveDelay);
            }
            if (!newSlaves.equals(this.slaves)) {
                TdsqlDirectLoggerFactory.logDebug(
                        "DataSet slave have changed, old: " + this.slaves + ", new: " + newSlaves);
                propertyChangeSupport.firePropertyChange(SLAVES_PROPERTY_NAME,
                        TdsqlDataSetUtil.copyDataSetList(this.slaves),
                        TdsqlDataSetUtil.copyDataSetList(newSlaves));
                this.slaves.clear();
                this.slaves.addAll(newSlaves);
                if (!slaveCached) {
                    slaveCached = true;
                }
                TdsqlDirectLoggerFactory.logDebug("After update, slaves is: " + this.slaves);
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }
    }

    public boolean isCached() {
        return masterCached && slaveCached && !TdsqlDirectTopoServer.getInstance().getScheduleQueue().isEmpty();
    }

    private static class SingletonInstance {

        public static final TdsqlDataSetCache INSTANCE = new TdsqlDataSetCache();
    }

    public static TdsqlDataSetCache getInstance() {
        return SingletonInstance.INSTANCE;
    }
}
