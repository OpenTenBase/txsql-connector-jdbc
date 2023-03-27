package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import java.util.Set;

/**
 * <p>TDSQL专属，负载均衡策略接口</p>
 *
 * @author dorianzhang@tencent.com
 */
public interface TdsqlLoadBalanceStrategy<T extends TdsqlConnectionCounter> {

    /**
     * 根据不同的负载均衡策略算法实现，从集合中计算并选择一个返回
     *
     * @param counterSet 待选择的连接计数器集合
     * @return 选择后的值
     */
    TdsqlConnectionCounter choice(Set<T> counterSet);
}
