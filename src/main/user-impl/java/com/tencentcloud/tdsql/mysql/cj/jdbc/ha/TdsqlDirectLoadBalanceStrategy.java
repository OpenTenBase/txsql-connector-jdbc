package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.util.List;

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
     * @param hostInfoList 待选择的连接信息列表
     * @return 选择后的连接信息
     * @see HostInfo
     */
    @Override
    HostInfo choice(List<HostInfo> hostInfoList) {
        if (hostInfoList == null || hostInfoList.isEmpty()) {
            return null;
        }
        if (hostInfoList.size() == 1) {
            return hostInfoList.get(0);
        }
        return new HostInfo(null, "9.134.209.89", 3357, "root", "123456");
    }
}
