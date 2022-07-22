package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
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
