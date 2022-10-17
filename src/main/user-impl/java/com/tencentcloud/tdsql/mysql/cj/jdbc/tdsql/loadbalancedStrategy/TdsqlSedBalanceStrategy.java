package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode.DIRECT;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode.LOAD_BALANCE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceBlacklistHolder;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalance.TdsqlLoadBalanceConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.NodeMsg;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>最短期望延迟算法策略</p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public class TdsqlSedBalanceStrategy implements TdsqlLoadBalanceStrategy {

    /**
     * <p>
     * 从连接信息列表中，根据具体负载均衡算法策略的实现逻辑，选取一个连接信息
     * </p>
     *
     * @param scheduleQueue 连接信息列表
     * @return 选择后的连接信息
     * @see TdsqlHostInfo
     */
    @Override
    public TdsqlHostInfo choice(TdsqlAtomicLongMap<TdsqlHostInfo> scheduleQueue) {
        if (scheduleQueue == null || scheduleQueue.isEmpty()) {
            return null;
        }

        Map<TdsqlHostInfo, NodeMsg> tdsqlHostInfoLongMap = scheduleQueue.asMap();
        if (scheduleQueue.size() == 1) {
            for (Entry<TdsqlHostInfo, NodeMsg> entry : tdsqlHostInfoLongMap.entrySet()) {
                return entry.getKey();
            }
        }
        int numHosts = tdsqlHostInfoLongMap.size();

        ReentrantReadWriteLock counterLock = TdsqlLoadBalanceConnectionCounter.getInstance().getCounterLock();
        counterLock.readLock().lock();
        try {
            List<Map.Entry<TdsqlHostInfo, NodeMsg>> counterList = new ArrayList<>(tdsqlHostInfoLongMap.entrySet());

            for (int i = 0; i < numHosts; i++) {
                Entry<TdsqlHostInfo, NodeMsg> curr = counterList.get(i);
                int currWf = curr.getKey().getWeightFactor();
                if (currWf > 0) {
                    long currCount = curr.getValue().getCount() + 1;
                    for (int j = i + 1; j < numHosts; j++) {
                        Entry<TdsqlHostInfo, NodeMsg> next = counterList.get(j);
                        int nextWf = next.getKey().getWeightFactor();
                        if (nextWf > 0) {
                            long nextCount = next.getValue().getCount() + 1;
                            if (currCount * nextWf >= nextCount * currWf) {
                                i = j;
                            }
                        }
                    }
                    TdsqlHostInfo choice = counterList.get(i).getKey();
                    logInfo("SED algorithm choice: " + choice.getHostPortPair());
                    if (LOAD_BALANCE.equals(choice.getConnectionMode())) {
                        logInfo("Current counter: " + TdsqlLoadBalanceConnectionCounter.getInstance().printCounter());
                        TdsqlLoadBalanceBlacklistHolder blacklistHolder = TdsqlLoadBalanceBlacklistHolder.getInstance();
                        if (blacklistHolder.isBlacklistEnabled()) {
                            logInfo("Current blacklist: " + blacklistHolder.printBlacklist());
                        }
                    } else if (DIRECT.equals(choice.getConnectionMode())) {
                        logInfo("Current counter: " + scheduleQueue);
                    }
                    return choice;
                }
            }
        } finally {
            counterLock.readLock().unlock();
        }
        return null;
    }
}
