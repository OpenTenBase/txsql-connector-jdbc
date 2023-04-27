package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlUnImplementMethodException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.MasterResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResultSet;

/**
 * <p>TDSQL专属，直连模式故障转移处理接口</p>
 *
 * @author dorianzhang@tencent.com
 */
public interface TdsqlDirectFailoverHandler {

    /**
     * 处理主库故障转移
     *
     * @param masterResult 主库拓扑信息比较结果
     */
    default void handleMaster(MasterResult masterResult) {
        throw TdsqlExceptionFactory.createException(TdsqlUnImplementMethodException.class,
                Messages.getString("TdsqlUnImplementMethodException",
                        new Object[]{"TdsqlDirectFailoverHandler.handleMaster()"}));
    }

    /**
     * 处理备库故障转移
     *
     * @param slaveResultSet 备库拓扑信息比较结果
     */
    default void handleSlaves(SlaveResultSet<SlaveResult> slaveResultSet) {
        throw TdsqlExceptionFactory.createException(TdsqlUnImplementMethodException.class,
                Messages.getString("TdsqlUnImplementMethodException",
                        new Object[]{"TdsqlDirectFailoverHandler.handleSlaves()"}));
    }
}
