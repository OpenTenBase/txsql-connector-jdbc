package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.listener;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.*;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 监听DataSetCache中的主从变化, 执行相应的failover操作.
 */
public class TdsqlFailoverTdsqlCacheListener extends AbstractTdsqlCacheListener {

    private final String tdsqlReadWriteMode;

    public TdsqlFailoverTdsqlCacheListener(String tdsqlReadWriteMode) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
    }

    /**
     * 主库变化
     */
    @Override
    public void handleMaster(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
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
    public void handleSlave(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
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
