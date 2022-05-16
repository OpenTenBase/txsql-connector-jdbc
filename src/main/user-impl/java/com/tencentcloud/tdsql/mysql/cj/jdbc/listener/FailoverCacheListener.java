package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlDirectFailoverOperator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectMasterSlaveSwitchMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 监听DataSetCache中的主从变化, 执行相应的failover操作.
 */
public class FailoverCacheListener extends AbstractCacheListener {

    private final String tdsqlReadWriteMode;

    public FailoverCacheListener(String tdsqlReadWriteMode) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
    }

    /**
     * 主库变化
     *
     * @param evt 属性变化事件
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleMaster(PropertyChangeEvent evt) {
        List<DataSetInfo> newMasters = (List<DataSetInfo>) evt.getNewValue();
        TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                TdsqlDirectMasterSlaveSwitchMode.MASTER_SLAVE_SWITCH, new ArrayList<>());
    }

    /**
     * 从库变化
     *
     * @param evt 属性变化事件
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleSlave(PropertyChangeEvent evt) {
        List<DataSetInfo> oldSlaves = (List<DataSetInfo>) evt.getOldValue();
        List<DataSetInfo> newSlaves = (List<DataSetInfo>) evt.getNewValue();
        List<DataSetInfo> offLineSlaves = new ArrayList<>(oldSlaves);
        offLineSlaves.removeAll(newSlaves);
        List<DataSetInfo> onlineSlaves = new ArrayList<>(newSlaves);
        onlineSlaves.removeAll(oldSlaves);

        if (offLineSlaves.size() > 0) {
            List<String> toCloseList = offLineSlaves.stream().map(d -> String.format("%s:%s", d.getIP(), d.getPort()))
                    .collect(Collectors.toList());
            TdsqlDirectLoggerFactory.logDebug("to close offline slave size: " + toCloseList.size());
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_OFFLINE, toCloseList);
        }

        if (onlineSlaves.size() > 0) {
            List<String> toCloseList = new ArrayList<>();
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_ONLINE, toCloseList);
        }
    }
}
