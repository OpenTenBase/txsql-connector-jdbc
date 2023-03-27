package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.algorithm;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * <p>TDSQL专属，负载均衡策略，最短期望延迟算法</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlSedAlgorithm<T extends TdsqlConnectionCounter> implements TdsqlLoadBalanceStrategy<T> {

    /**
     * {@inheritDoc}
     *
     * @param counterSet 待选择的连接计数器集合
     * @return
     */
    @Override
    public TdsqlConnectionCounter choice(Set<T> counterSet) {
        if (counterSet.size() == 1) {
            for (T t : counterSet) {
                t.getCount().increment();
                return t;
            }
        }

        List<T> counterList = new ArrayList<>(counterSet);
        int numHosts = counterList.size();

        for (int i = 0; i < numHosts; i++) {
            TdsqlConnectionCounter currentCounter = counterList.get(i);
            Integer currentWeight = currentCounter.getTdsqlHostInfo().getWeight();
            if (currentWeight != null && currentWeight > 0) {
                int currentCount = currentCounter.getCount().intValue();
                for (int j = i + 1; j < numHosts; j++) {
                    TdsqlConnectionCounter nextCounter = counterList.get(j);
                    Integer nextWeight = nextCounter.getTdsqlHostInfo().getWeight();
                    if (nextWeight != null && nextWeight > 0) {
                        int nextCount = nextCounter.getCount().intValue() + 1;
                        if (currentCount * nextWeight >= nextCount * currentWeight) {
                            i = j;
                        }
                    }
                }
                T t = counterList.get(i);
                t.getCount().increment();
                return t;
            }
        }
        return null;
    }
}
