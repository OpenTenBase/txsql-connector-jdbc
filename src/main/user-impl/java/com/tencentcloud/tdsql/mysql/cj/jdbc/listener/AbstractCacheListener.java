package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCache;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public abstract class AbstractCacheListener implements PropertyChangeListener {

    abstract void handleMaster(PropertyChangeEvent evt);

    abstract void handleSlave(PropertyChangeEvent evt);

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        TdsqlDirectTopoServer.getInstance().getRefreshLock().writeLock().lock();
        try {
            if (evt.getPropertyName().equals(DataSetCache.MASTERS_PROPERTY_NAME)) {
                handleMaster(evt);
            } else if (evt.getPropertyName().equals(DataSetCache.SLAVES_PROPERTY_NAME)) {
                handleSlave(evt);
            }
        } finally {
            TdsqlDirectTopoServer.getInstance().getRefreshLock().writeLock().unlock();
        }
    }
}
