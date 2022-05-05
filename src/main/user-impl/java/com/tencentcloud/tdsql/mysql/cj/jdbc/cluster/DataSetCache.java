package com.tencentcloud.tdsql.mysql.cj.jdbc.cluster;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
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
    private boolean cached = false;

    private static final String MASTERS_PROPERTY_NAME = "masters";
    private static final String SLAVES_PROPERTY_NAME = "slaves";

    public DataSetCache() {
    }

    /**
     * 等待第一次缓存完成.
     *
     * @param interval 重试间隔(秒)
     * @param count    重试次数
     * @return 是否第一次缓存完成
     */
    public boolean waitCached(int interval, int count){
        try {
            WaitUtil.waitFor(interval, count, this::isCached);
        } catch (InterruptedException ignored) {}
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
        if(!masters.equals(this.masters)) {
            propertyChangeSupport.firePropertyChange(MASTERS_PROPERTY_NAME, this.masters, masters);
            this.masters = masters;
        }
    }

    public List<DataSetInfo> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<DataSetInfo> slaves) {
        if(!slaves.equals(this.slaves)) {
            propertyChangeSupport.firePropertyChange(SLAVES_PROPERTY_NAME, this.slaves, slaves);
            this.slaves = slaves;
        }
    }

    public boolean isCached() {
        return cached;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

    private static class SingletonInstance {

        private static final DataSetCache INSTANCE = new DataSetCache();
    }
}
