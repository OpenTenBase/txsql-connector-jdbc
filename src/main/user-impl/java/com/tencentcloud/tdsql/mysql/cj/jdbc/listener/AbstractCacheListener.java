package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class AbstractCacheListener implements PropertyChangeListener {

    private List<DataSetInfo>[] getSplitList(PropertyChangeEvent evt) {
        List<DataSetInfo>[] res = new List[2];
        List<DataSetInfo> oldList = (List<DataSetInfo>) evt.getOldValue();
        List<DataSetInfo> newList = (List<DataSetInfo>) evt.getNewValue();
        List<DataSetInfo> offLines = new ArrayList<>(oldList);
        offLines.removeAll(newList);
        List<DataSetInfo> onlines = new ArrayList<>(newList);
        onlines.removeAll(oldList);
        res[0] = offLines;
        res[1] = onlines;
        return res;
    }

    abstract void handleMaster(List<DataSetInfo> offLines, List<DataSetInfo> onLines);

    abstract void handleSlave(List<DataSetInfo> offLines, List<DataSetInfo> onLines);

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        ReentrantReadWriteLock refreshLock = TdsqlDirectTopoServer.getInstance().getRefreshLock();
        refreshLock.writeLock().lock();
        List<DataSetInfo>[] res = getSplitList(evt);
        try {
            if (evt.getPropertyName().equals(DataSetCache.MASTERS_PROPERTY_NAME)) {
                handleMaster(res[0], res[1]);
            } else if (evt.getPropertyName().equals(DataSetCache.SLAVES_PROPERTY_NAME)) {
                handleSlave(res[0], res[1]);
            }
        } finally {
            refreshLock.writeLock().unlock();
        }
    }
}
