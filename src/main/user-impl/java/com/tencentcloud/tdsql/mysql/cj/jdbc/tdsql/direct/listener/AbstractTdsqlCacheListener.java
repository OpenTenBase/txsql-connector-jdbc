package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.listener;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetInfo;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class AbstractTdsqlCacheListener implements PropertyChangeListener {

    private List<TdsqlDataSetInfo>[] getSplitList(PropertyChangeEvent evt) {
        List<TdsqlDataSetInfo>[] res = new List[2];
        List<TdsqlDataSetInfo> oldList = (List<TdsqlDataSetInfo>) evt.getOldValue();
        List<TdsqlDataSetInfo> newList = (List<TdsqlDataSetInfo>) evt.getNewValue();
        List<TdsqlDataSetInfo> offLines = new ArrayList<>(oldList);
        offLines.removeAll(newList);
        List<TdsqlDataSetInfo> onlines = new ArrayList<>(newList);
        onlines.removeAll(oldList);
        res[0] = offLines;
        res[1] = onlines;
        return res;
    }

    abstract void handleMaster(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines);

    abstract void handleSlave(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines);

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        ReentrantReadWriteLock refreshLock = TdsqlDirectTopoServer.getInstance().getRefreshLock();
        refreshLock.writeLock().lock();
        List<TdsqlDataSetInfo>[] res = getSplitList(evt);
        try {
            if (evt.getPropertyName().equals(TdsqlDataSetCache.MASTERS_PROPERTY_NAME)) {
                handleMaster(res[0], res[1]);
            } else if (evt.getPropertyName().equals(TdsqlDataSetCache.SLAVES_PROPERTY_NAME)) {
                handleSlave(res[0], res[1]);
            }
        } finally {
            refreshLock.writeLock().unlock();
        }
    }
}
