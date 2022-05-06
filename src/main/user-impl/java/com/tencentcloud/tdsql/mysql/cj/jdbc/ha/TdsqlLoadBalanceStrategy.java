package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import java.util.List;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public abstract class TdsqlLoadBalanceStrategy {

    /**
     * <p>
     * 从连接信息列表中，根据具体负载均衡算法策略的实现逻辑，选取一个连接信息
     * </p>
     *
     * @return 选择后的连接信息
     * @see HostInfo
     */
    abstract HostInfo choice();

    protected TdsqlHostInfo getRandom(List<TdsqlHostInfo> hostInfoList) {
        int random = (int) Math.floor((Math.random() * hostInfoList.size()));
        return hostInfoList.get(random);
    }
}
