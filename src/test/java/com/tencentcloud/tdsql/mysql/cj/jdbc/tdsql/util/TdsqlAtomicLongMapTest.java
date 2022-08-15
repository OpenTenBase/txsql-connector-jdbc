package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy.TdsqlDirectLoadBalanceStrategyFactory;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;


class TdsqlAtomicLongMapTest {
    @Test
    public void TestMap() throws SQLException {
        TdsqlAtomicLongMap<TdsqlHostInfo> map = TdsqlAtomicLongMap.create();
        HostInfo hostInfo1 = new HostInfo(null, "localhost", 3307, "seimin", "www", null);
        HostInfo hostInfo2 = new HostInfo(null, "slave1", 3308, "seimin", "www", null);
        HostInfo hostInfo3 = new HostInfo(null, "slave2", 3308, "seimin", "www", null);
        HostInfo hostInfo4 = new HostInfo(null, "slave3", 3308, "seimin", "www", null);
        HostInfo hostInfo5 = new HostInfo(null, "slave4", 3308, "seimin", "www", null);
        HostInfo hostInfo6 = new HostInfo(null, "slave5", 3308, "seimin", "www", null);
        TdsqlHostInfo tdsqlHostInfo1 = new TdsqlHostInfo(hostInfo1);
        tdsqlHostInfo1.setWeightFactor(0);
        TdsqlHostInfo tdsqlHostInfo2 = new TdsqlHostInfo(hostInfo2);
        tdsqlHostInfo2.setWeightFactor(0);
        TdsqlHostInfo tdsqlHostInfo3 = new TdsqlHostInfo(hostInfo3);
        tdsqlHostInfo3.setWeightFactor(0);
        TdsqlHostInfo tdsqlHostInfo4 = new TdsqlHostInfo(hostInfo4);
        tdsqlHostInfo4.setWeightFactor(0);
        TdsqlHostInfo tdsqlHostInfo5 = new TdsqlHostInfo(hostInfo5);
        tdsqlHostInfo5.setWeightFactor(2);
        TdsqlHostInfo tdsqlHostInfo6 = new TdsqlHostInfo(hostInfo6);
        tdsqlHostInfo6.setWeightFactor(1);
        map.put(tdsqlHostInfo1, new NodeMsg(1L, true));
        map.put(tdsqlHostInfo2, new NodeMsg(1L, false));
        map.put(tdsqlHostInfo3, new NodeMsg(2L, false));
        map.put(tdsqlHostInfo4, new NodeMsg(3L, false));
        map.put(tdsqlHostInfo5, new NodeMsg(4L, false));
        map.put(tdsqlHostInfo6, new NodeMsg(4L, false));
        System.out.println(map.get(tdsqlHostInfo1).getCount());
        TdsqlDirectLoadBalanceStrategyFactory instance = TdsqlDirectLoadBalanceStrategyFactory.getInstance();
        TdsqlLoadBalanceStrategy lc = instance.getStrategyInstance("Sed");
        TdsqlHostInfo choice = lc.choice(map);
        System.out.println(choice.getHost());
        System.out.println("null:" + map.remove(tdsqlHostInfo6));

    }

}