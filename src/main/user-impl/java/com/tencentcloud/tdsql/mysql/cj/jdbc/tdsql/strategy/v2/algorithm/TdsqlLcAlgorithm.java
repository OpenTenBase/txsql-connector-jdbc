package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.algorithm;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategy;
import java.util.Objects;
import java.util.Set;

/**
 * <p>TDSQL专属，负载均衡策略，最小连接数算法</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLcAlgorithm<T extends TdsqlConnectionCounter> implements TdsqlLoadBalanceStrategy<T> {

    /**
     * {@inheritDoc}
     *
     * @param counterSet 待选择的连接计数器集合
     * @return 选择后的值
     */
    @Override
    public synchronized TdsqlConnectionCounter choice(Set<T> counterSet) {
        if (counterSet.size() == 1) {
            for (T t : counterSet) {
                t.getCount().increment();
                return t;
            }
        }

        TdsqlConnectionCounter minimumCounter = null;
        for (TdsqlConnectionCounter counter : counterSet) {
            if (minimumCounter == null) {
                minimumCounter = counter;
                continue;
            }
            if (counter.getCount().longValue() < minimumCounter.getCount().longValue()) {
                minimumCounter = counter;
            }
        }
        Objects.requireNonNull(minimumCounter).getCount().increment();
        return minimumCounter;
    }
}
