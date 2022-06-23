package com.tencentcloud.tdsql.mysql.cj.jdbc.cluster;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.WaitUtil;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 所有数据节点缓存.
 */
public class DataSetCache {

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    private final List<DataSetInfo> masters = new CopyOnWriteArrayList<>();
    private final List<DataSetInfo> slaves = new CopyOnWriteArrayList<>();
    private boolean masterCached = false;
    private boolean slaveCached = false;

    public static final String MASTERS_PROPERTY_NAME = "masters";
    public static final String SLAVES_PROPERTY_NAME = "slaves";

    private DataSetCache() {
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
            WaitUtil.waitFor(interval, count, this::isCached);
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

    public synchronized List<DataSetInfo> getMasters() {
        TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().lock();
        try {
            return masters;
        } finally {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().unlock();
        }
    }

    public synchronized void setMasters(List<DataSetInfo> newMasters) {
        if (!newMasters.equals(this.masters)) {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().writeLock().lock();
            try {
                TdsqlDirectLoggerFactory.logDebug(
                        "DataSet master have changed, old: " + this.masters + ", new: " + newMasters);
                propertyChangeSupport.firePropertyChange(MASTERS_PROPERTY_NAME,
                        DataSetUtil.copyDataSetList(this.masters), DataSetUtil.copyDataSetList(newMasters));
                this.masters.clear();
                this.masters.addAll(newMasters);
                TdsqlDirectLoggerFactory.logDebug("After update, master is: " + this.masters);
                if (!masterCached) {
                    masterCached = true;
                }
            } finally {
                TdsqlDirectTopoServer.getInstance().getRefreshLock().writeLock().unlock();
            }
        }
    }

    public synchronized List<DataSetInfo> getSlaves() {
        TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().lock();
        try {
            return slaves;
        } finally {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().readLock().unlock();
        }
    }

    public synchronized void setSlaves(List<DataSetInfo> newSlaves) {
        TdsqlDirectTopoServer topoServer = TdsqlDirectTopoServer.getInstance();
        topoServer.getRefreshLock().writeLock().lock();
        try {
            Integer tdsqlMaxSlaveDelay = topoServer.getTdsqlMaxSlaveDelay();
            if (tdsqlMaxSlaveDelay > 0) {
                newSlaves.removeIf(dsInfo -> dsInfo.getDelay() >= tdsqlMaxSlaveDelay);
            }
            if (!newSlaves.equals(this.slaves)) {
                TdsqlDirectLoggerFactory.logDebug(
                        "DataSet slave have changed, old: " + this.slaves + ", new: " + newSlaves);
                propertyChangeSupport.firePropertyChange(SLAVES_PROPERTY_NAME, DataSetUtil.copyDataSetList(this.slaves),
                        DataSetUtil.copyDataSetList(newSlaves));
                this.slaves.clear();
                this.slaves.addAll(newSlaves);
                TdsqlDirectLoggerFactory.logDebug("After update, slaves is: " + this.masters);
                if (!slaveCached) {
                    slaveCached = true;
                }
            }
        } finally {
            topoServer.getRefreshLock().writeLock().unlock();
        }
    }

    public boolean isCached() {
        return masterCached && slaveCached;
    }

    private static class SingletonInstance {

        public static final DataSetCache INSTANCE = new DataSetCache();
    }

    public static DataSetCache getInstance() {
        return SingletonInstance.INSTANCE;
    }
}
