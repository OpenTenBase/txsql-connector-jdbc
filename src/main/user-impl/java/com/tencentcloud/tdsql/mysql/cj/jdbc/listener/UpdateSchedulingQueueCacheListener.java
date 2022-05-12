package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlAtomicLongMap;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;

public class UpdateSchedulingQueueCacheListener extends AbstractCacheListener {

    private final TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue;
    private final String tdsqlReadWriteMode;
    private final ConnectionUrl connectionUrl;

    public UpdateSchedulingQueueCacheListener(String tdsqlReadWriteMode,
            TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue, ConnectionUrl connectionUrl) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
        this.scheduleQueue = scheduleQueue;
        this.connectionUrl = connectionUrl;
    }

    /**
     * 主库变化
     * 1. 如果当前模式是RW, 将新主库加到schedulingQueue
     * 2. 如果当前模式是RO, 将新主库加到schedulingQueue
     *
     * @param evt 属性变化事件
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleMaster(PropertyChangeEvent evt) {
        List<DataSetInfo> newMasters = (List<DataSetInfo>) evt.getNewValue();
        if (TdsqlDirectReadWriteMode.RW.equals(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode))) {
            for (DataSetInfo newMaster : newMasters) {
                TdsqlHostInfo tdsqlHostInfo = DataSetUtil.convertDataSetInfo(newMaster, connectionUrl);
                if (!scheduleQueue.containsKey(tdsqlHostInfo)) {
                    scheduleQueue.put(tdsqlHostInfo, 0L);
                }
            }
        }
    }

    /**
     * 从库变化
     * 1. 如果当前模式是RW, 不做操作
     * 2. 如果当前模式是RO, 将所有从库加到schedulingQueue
     *
     * @param evt 属性变化事件
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleSlave(PropertyChangeEvent evt) {
        List<DataSetInfo> newSlaves = (List<DataSetInfo>) evt.getNewValue();
        if (TdsqlDirectReadWriteMode.RW.equals(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode))) {
            // ignored
        }
        if (TdsqlDirectReadWriteMode.RO.equals(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode))) {
            for (DataSetInfo slave : newSlaves) {
                TdsqlHostInfo tdsqlHostInfo = DataSetUtil.convertDataSetInfo(slave, connectionUrl);
                if (!scheduleQueue.containsKey(tdsqlHostInfo)) {
                    scheduleQueue.put(tdsqlHostInfo, 0L);
                }
            }
        }
    }
}
