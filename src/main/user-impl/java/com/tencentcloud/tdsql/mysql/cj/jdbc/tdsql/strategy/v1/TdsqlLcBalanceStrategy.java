package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v1;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum.DIRECT;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum.LOAD_BALANCE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.loadbalance.TdsqlLoadBalanceConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.NodeMsg;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlAtomicLongMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * <p>最小连接算法策略</p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public final class TdsqlLcBalanceStrategy implements TdsqlLoadBalanceStrategy {

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
        if (scheduleQueue.size() == 1) {
            for (Entry<TdsqlHostInfo, NodeMsg> entry : scheduleQueue.asMap().entrySet()) {
                return entry.getKey();
            }
        }
        ReentrantReadWriteLock counterLock = TdsqlLoadBalanceConnectionCounter.getInstance().getCounterLock();
        counterLock.readLock().lock();
        try {
            List<TdsqlHostInfo> tdsqlHostInfoList = Collections.unmodifiableList(
                    new ArrayList<>(scheduleQueue.asMap().keySet()));
            List<Long> countList = Collections.unmodifiableList(
                    tdsqlHostInfoList.stream().map(scheduleQueue::get).map(NodeMsg::getCount)
                            .collect(Collectors.toList()));
            int minIndex = countList.indexOf(Collections.min(countList));
            TdsqlHostInfo choice = tdsqlHostInfoList.get(minIndex);
            logInfo("[" + choice.getOwnerUuid() + "] Lc algorithm choice: " + choice.getHostPortPair());
            if (LOAD_BALANCE.equals(choice.getConnectionMode())) {
                logInfo("[" + choice.getOwnerUuid() + "] Current counter: "
                        + TdsqlLoadBalanceConnectionCounter.getInstance().printCounter());
            } else if (DIRECT.equals(choice.getConnectionMode())) {
                logInfo("[" + choice.getOwnerUuid() + "] Current counter: " + scheduleQueue);
            }
            return choice;
        } finally {
            counterLock.readLock().unlock();
        }
    }
}
