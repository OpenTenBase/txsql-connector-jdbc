package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SWITCH;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.MasterResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectHandleFailoverException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;

/**
 * <p>TDSQL专属，直连模式主库故障转移处理器</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectFailoverMasterHandler implements TdsqlDirectFailoverHandler {

    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final TdsqlDirectScheduleServer scheduleServer;
    private final TdsqlDirectConnectionManager connectionManager;

    public TdsqlDirectFailoverMasterHandler(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
        this.scheduleServer = dataSourceConfig.getScheduleServer();
        this.connectionManager = dataSourceConfig.getConnectionManager();
    }

    /**
     * 处理主库故障转移
     *
     * @param masterResult 主库拓扑信息比较结果
     */
    @Override
    public void handleMaster(MasterResult masterResult) {
        // 无变化抛出异常
        if (masterResult.isNoChange()) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.NoChangeForMaster"));
        }

        // 变化类型应该为主备切换，否则抛出异常
        if (!SWITCH.equals(masterResult.getTdsqlDirectTopoChangeEventEnum())) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectHandleFailoverException.class,
                    Messages.getString("TdsqlDirectHandleFailoverException.UnknownChangeEventForMaster"));
        }

        TdsqlDirectHostInfo oldMaster = masterResult.getOldMaster().convertToDirectHostInfo(this.dataSourceConfig);
        TdsqlDirectHostInfo newMaster = masterResult.getNewMaster().convertToDirectHostInfo(this.dataSourceConfig);

        // 调用调度服务，更新主库调度信息
        this.scheduleServer.updateMaster(oldMaster, newMaster);

        // 调用连接管理器，关闭所有老主库存量连接
        this.connectionManager.closeAllConnection(oldMaster);
    }
}
