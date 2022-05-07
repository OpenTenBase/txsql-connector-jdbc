package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;

public class UpdateSchedulingQueueCacheListener implements PropertyChangeListener {

    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue;
    private final String tdsqlReadWriteMode;
    private final ConnectionUrl connectionUrl;

    public UpdateSchedulingQueueCacheListener(String tdsqlReadWriteMode, TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue, ConnectionUrl connectionUrl) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
        this.scheduleQueue = scheduleQueue;
        this.connectionUrl = connectionUrl;
    }

    /**
     * 主库变化
     * 1. 如果当前模式是RW, 将新主库加到schedulingQueue
     * 2. 如果当前模式是RO, 将新主库和所有从库加到schedulingQueue
     * @param evt 属性变化事件
     */
    private void handleMaster(PropertyChangeEvent evt) {
        List<DataSetInfo> newMasters = (List<DataSetInfo>)evt.getNewValue();
        if(tdsqlReadWriteMode.equals(TdsqlDirectReadWriteMode.RW.name())) {
            scheduleQueue.clear();
            for (DataSetInfo newMaster : newMasters) {
                scheduleQueue.put(DataSetUtil.convertDataSetInfo(newMaster, connectionUrl), 0L);
            }
        }
        if(tdsqlReadWriteMode.equals(TdsqlDirectReadWriteMode.RO.name())) {
            scheduleQueue.clear();
            for (DataSetInfo newMaster : newMasters) {
                scheduleQueue.put(DataSetUtil.convertDataSetInfo(newMaster, connectionUrl), 0L);
            }
            for (DataSetInfo slave : DataSetCache.SingletonInstance.INSTANCE.getSlaves()) {
                scheduleQueue.put(DataSetUtil.convertDataSetInfo(slave, connectionUrl), 0L);
            }
        }
    }

    /**
     * 从库变化
     * 1. 如果当前模式是RW, 不做操作
     * 2. 如果当前模式是RO, 将所有从库加到schedulingQueue
     * @param evt 属性变化事件
     */
    private void handleSlave(PropertyChangeEvent evt) {
        List<DataSetInfo> newSlaves = (List<DataSetInfo>)evt.getNewValue();
        if(tdsqlReadWriteMode.equals(TdsqlDirectReadWriteMode.RW.name())) {
            // ignored
        }
        if(tdsqlReadWriteMode.equals(TdsqlDirectReadWriteMode.RO.name())) {
            scheduleQueue.clear();
            for (DataSetInfo slave : newSlaves) {
                scheduleQueue.put(DataSetUtil.convertDataSetInfo(slave, connectionUrl), 0L);
            }
        }
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        if(evt.getPropertyName().equals(DataSetCache.MASTERS_PROPERTY_NAME)){
            handleMaster(evt);
        } else if(evt.getPropertyName().equals(DataSetCache.SLAVES_PROPERTY_NAME)) {
            handleSlave(evt);
        }
    }
}
