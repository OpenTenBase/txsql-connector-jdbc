package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RW;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectBlacklistHolder;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConnectionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlWaitUtil;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.Map;
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
            // 当获取到的主库拓扑信息为空的时候，需要分多种情况判断
            String tdsqlDirectReadWriteMode = topoServer.getTdsqlDirectReadWriteMode();
            if (newMasters.isEmpty()) {
                // 只读模式，不再需要执行更新缓存的代码逻辑
                if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode)) {
                    TdsqlDirectLoggerFactory.logWarn(
                            "After update, master is empty, but we in RO mode, so to be continue!");
                    if (!masterCached) {
                        masterCached = true;
                    }
                    return;
                } else if (TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(tdsqlDirectReadWriteMode)
                        && this.masters.isEmpty()) {
                    // 读写模式，且缓存的主库拓扑信息也为空，不再需要执行更新缓存的代码逻辑
                    TdsqlDirectLoggerFactory.logWarn(
                            "After update, master is empty, although we in RW mode, cached master also empty, so to be continue!");
                    if (!masterCached) {
                        masterCached = true;
                    }
                    return;
                }
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
            // 当获取到的从库拓扑信息为空的时候，需要分多种情况判断
            String tdsqlDirectReadWriteMode = topoServer.getTdsqlDirectReadWriteMode();
            if (newSlaves.isEmpty()) {
                // 读写模式，不再需要执行更新缓存的代码逻辑
                if (TDSQL_DIRECT_READ_WRITE_MODE_RW.equalsIgnoreCase(tdsqlDirectReadWriteMode)) {
                    TdsqlDirectLoggerFactory.logWarn(
                            "After update, slaves is empty, but we in RW mode, so to be continue!");
                    if (!slaveCached) {
                        slaveCached = true;
                    }
                    return;
                } else if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode)
                        && this.slaves.isEmpty()) {
                    // 只读模式，且缓存的从库拓扑信息也为空，不再需要执行更新缓存的代码逻辑
                    TdsqlDirectLoggerFactory.logWarn(
                            "After update, slaves is empty, although we in RO mode, cached slaves also empty, so to be continue!");
                    if (!slaveCached) {
                        slaveCached = true;
                    }
                    return;
                }
            }

            Integer tdsqlMaxSlaveDelay = topoServer.getTdsqlDirectMaxSlaveDelaySeconds();
            ConnectionUrl connectionUrl = topoServer.getConnectionUrl();
            //如果设置了从库最大延迟并且数据库实延迟大于这个设定的延迟 或者 没有设定但是延迟大于10秒，我们都将这个节点认为不可调
            for (TdsqlDataSetInfo newSlave : newSlaves){
                if ((tdsqlMaxSlaveDelay > 0 && newSlave.getDelay() >= tdsqlMaxSlaveDelay) || newSlave.getDelay() > 100000){
                    newSlaves.remove(newSlave);
                    TdsqlHostInfo tdsqlHostInfo = TdsqlDataSetUtil.convertDataSetInfo(newSlave, connectionUrl);
                    TdsqlDirectBlacklistHolder.getInstance().addBlacklist(tdsqlHostInfo);
                }
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
            if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(tdsqlDirectReadWriteMode) && newSlaves.isEmpty() && this.slaves.isEmpty()){
                TdsqlDirectLoggerFactory.logDebug(
                        "DataSet slave is null! but in ReadOnly mode, So NOOP!" );
                if (!slaveCached) {
                    slaveCached = true;
                }
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
