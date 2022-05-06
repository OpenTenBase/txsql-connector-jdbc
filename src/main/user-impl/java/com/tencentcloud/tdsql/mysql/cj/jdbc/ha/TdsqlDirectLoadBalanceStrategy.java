package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlDirectLoadBalanceStrategy extends TdsqlLoadBalanceStrategy {

    /**
     * <p>
     * 从连接信息列表中，根据具体负载均衡算法策略的实现逻辑，选取一个连接信息
     * </p>
     *
     * @return 选择后的连接信息
     * @see HostInfo
     */
    @Override
    public HostInfo choice() {
        ConcurrentHashMap<TdsqlHostInfo, Long> scheduleQueue = TdsqlDirectTopoServer.getInstance().getScheduleQueue();
        if (scheduleQueue == null || scheduleQueue.isEmpty()) {
            return null;
        }
        if (scheduleQueue.size() == 1) {
            return scheduleQueue.keys().nextElement();
        }

        List<TdsqlHostInfo> tdsqlHostInfoList = Collections.unmodifiableList(new LinkedList<>(scheduleQueue.keySet()));
        List<Long> countList = Collections.unmodifiableList(tdsqlHostInfoList.stream().map(scheduleQueue::get)
                .collect(Collectors.toList()));
        int minIndex = countList.indexOf(Collections.min(countList));
        return tdsqlHostInfoList.get(minIndex);
    }
}
