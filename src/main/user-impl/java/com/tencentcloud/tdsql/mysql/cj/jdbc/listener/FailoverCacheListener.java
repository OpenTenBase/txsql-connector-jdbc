package com.tencentcloud.tdsql.mysql.cj.jdbc.listener;

import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.TdsqlDirectFailoverOperator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectMasterSlaveSwitchMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
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
     */
    @Override
    public void handleMaster(List<DataSetInfo> offLines, List<DataSetInfo> onLines) {
        if (!offLines.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("Offline master: " + offLines);
            List<String> toCloseList = offLines.stream().map(d -> String.format("%s:%s", d.getIp(), d.getPort()))
                    .collect(Collectors.toList());
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.MASTER_SLAVE_SWITCH, toCloseList);
        }
    }

    /**
     * 从库变化
     */
    @Override
    public void handleSlave(List<DataSetInfo> offLines, List<DataSetInfo> onLines) {
        if (!offLines.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("Offline slaves: " + offLines);
            List<String> toCloseList = offLines.stream().map(d -> String.format("%s:%s", d.getIp(), d.getPort()))
                    .collect(Collectors.toList());
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_OFFLINE, toCloseList);
        }
        if (!onLines.isEmpty()) {
            TdsqlDirectLoggerFactory.logDebug("Online slaves: " + onLines);
            List<String> toCloseList = new ArrayList<>();
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteMode.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_ONLINE, toCloseList);
        }
    }
}
