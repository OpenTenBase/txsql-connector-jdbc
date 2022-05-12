package com.tencentcloud.tdsql.mysql.cj.jdbc.cluster;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.WaitUtil;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;

/**
 * 所有数据节点缓存.
 */
public class DataSetCache {

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    private List<DataSetInfo> masters = new ArrayList<>();
    private List<DataSetInfo> slaves = new ArrayList<>();
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
        } catch (InterruptedException ignored) {
        }
        if(isCached()) {
            TdsqlDirectLoggerFactory.getLogger().logDebug("TDSQL data set cached.");
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

    public List<DataSetInfo> getMasters() {
        return masters;
    }

    public void setMasters(List<DataSetInfo> masters) {
        if (!masters.equals(this.masters)) {
            TdsqlDirectLoggerFactory.getLogger().logDebug("DataSet master have change");
            propertyChangeSupport.firePropertyChange(MASTERS_PROPERTY_NAME, this.masters, masters);
            this.masters = masters;
            if (!masterCached) {
                masterCached = true;
            }
        }
    }

    public List<DataSetInfo> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<DataSetInfo> slaves) {
        if (TdsqlDirectTopoServer.getInstance().getTdsqlMaxSlaveDelay() > 0) {
            slaves.removeIf(dataSetInfo -> dataSetInfo.getDelay() > TdsqlDirectTopoServer.getInstance()
                    .getTdsqlMaxSlaveDelay());
        }
        if (!slaves.equals(this.slaves)) {
            TdsqlDirectLoggerFactory.getLogger().logDebug("DataSet slave have change");
            propertyChangeSupport.firePropertyChange(SLAVES_PROPERTY_NAME, this.slaves, slaves);
            this.slaves = slaves;
            if (!slaveCached) {
                slaveCached = true;
            }
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
