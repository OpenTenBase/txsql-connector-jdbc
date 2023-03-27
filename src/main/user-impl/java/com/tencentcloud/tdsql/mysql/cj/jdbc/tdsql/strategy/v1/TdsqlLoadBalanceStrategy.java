package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v1;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;

/**
 * <p>负载均衡策略算法接口定义</p>
 *
 * @author dorianzhang@tencent.com
 * @see TdsqlLcBalanceStrategy
 * @see TdsqlSedBalanceStrategy
 */
public interface TdsqlLoadBalanceStrategy {

    /**
     * <p>
     * 从连接信息列表中，根据具体负载均衡算法策略的实现逻辑，选取一个连接信息
     * </p>
     *
     * @param scheduleQueue 连接信息列表
     * @return 选择后的连接信息
     * @see TdsqlHostInfo
     */
    TdsqlHostInfo choice(TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue);
}
