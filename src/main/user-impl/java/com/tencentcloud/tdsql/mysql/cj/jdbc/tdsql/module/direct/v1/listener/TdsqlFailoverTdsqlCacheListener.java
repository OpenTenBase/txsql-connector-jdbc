package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.listener;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.TdsqlDirectFailoverOperator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.TdsqlDirectMasterSlaveSwitchMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.datasource.TdsqlDirectDataSourceCounter;
import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 监听DataSetCache中的主从变化, 执行相应的failover操作.
 */
public class TdsqlFailoverTdsqlCacheListener extends AbstractTdsqlCacheListener {

    private final String tdsqlReadWriteMode;
    private final String ownerUuid;

    public TdsqlFailoverTdsqlCacheListener(String tdsqlReadWriteMode, String ownerUuid) {
        this.tdsqlReadWriteMode = tdsqlReadWriteMode;
        this.ownerUuid = ownerUuid;
    }

    /**
     * 属性变化监测，在子类中进行加锁
     *
     * @param evt 属性变化事件
     */
    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        ReentrantReadWriteLock refreshLock = TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(ownerUuid).
                getTopoServer().getRefreshLock();
        try {
            refreshLock.writeLock().lock();
            super.propertyChange(evt);
        } finally {
            refreshLock.writeLock().unlock();
        }
    }

    /**
     * 主库变化
     */
    @Override
    public void handleMaster(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
        if (!offLines.isEmpty()) {
            logInfo("[" + this.ownerUuid + "] Offline master: " + offLines);
            List<String> toCloseList = offLines.stream().map(d -> String.format("%s:%s", d.getIp(), d.getPort()))
                    .collect(Collectors.toList());
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteModeEnum.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.MASTER_SLAVE_SWITCH, toCloseList, this.ownerUuid);
        }
    }

    /**
     * 从库变化
     */
    @Override
    public void handleSlave(List<TdsqlDataSetInfo> offLines, List<TdsqlDataSetInfo> onLines) {
        if (!offLines.isEmpty()) {
            logInfo("[" + this.ownerUuid + "] Offline slaves: " + offLines);
            List<String> toCloseList = offLines.stream().map(d -> String.format("%s:%s", d.getIp(), d.getPort()))
                    .collect(Collectors.toList());
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteModeEnum.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_OFFLINE, toCloseList, this.ownerUuid);

        }
        if (!onLines.isEmpty()) {
            logInfo("[" + this.ownerUuid + "] Online slaves: " + onLines);
            List<String> toCloseList = new ArrayList<>();
            TdsqlDirectFailoverOperator.subsequentOperation(TdsqlDirectReadWriteModeEnum.convert(tdsqlReadWriteMode),
                    TdsqlDirectMasterSlaveSwitchMode.SLAVE_ONLINE, toCloseList, this.ownerUuid);
        }
    }
}
