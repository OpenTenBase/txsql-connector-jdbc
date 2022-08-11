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
        /**
         *     public static final int NO_PORT = -1;
         *     private static final String HOST_PORT_SEPARATOR = ":";
         *
         *     private final DatabaseUrlContainer originalUrl;
         *     private final String host;
         *     private final int port;
         *     private final String user;
         *     private final String password;
         *     private final Map<String, String> hostProperties = new HashMap<>();
         *
         *     public HostInfo(DatabaseUrlContainer url, String host, int port, String user, String password, Map<String, String> properties) {
         *         this.originalUrl = url;
         *         this.host = host;
         *         this.port = port;
         *         this.user = user;
         *         this.password = password;
         *         if (properties != null) {
         *             this.hostProperties.putAll(properties);
         *         }
         *     }
         */
        HostInfo hostInfo1 = new HostInfo(null, "localhost", 3307, "seimin", "www", null);
        HostInfo hostInfo2 = new HostInfo(null, "slave1", 3308, "seimin", "www", null);
        HostInfo hostInfo3 = new HostInfo(null, "slave2", 3308, "seimin", "www", null);
        HostInfo hostInfo4 = new HostInfo(null, "slave3", 3308, "seimin", "www", null);
        HostInfo hostInfo5 = new HostInfo(null, "slave4", 3308, "seimin", "www", null);
        HostInfo hostInfo6 = new HostInfo(null, "slave5", 3308, "seimin", "www", null);
        TdsqlHostInfo tdsqlHostInfo1 = new TdsqlHostInfo(hostInfo1);
        TdsqlHostInfo tdsqlHostInfo2 = new TdsqlHostInfo(hostInfo2);
        TdsqlHostInfo tdsqlHostInfo3 = new TdsqlHostInfo(hostInfo3);
        TdsqlHostInfo tdsqlHostInfo4 = new TdsqlHostInfo(hostInfo4);
        TdsqlHostInfo tdsqlHostInfo5 = new TdsqlHostInfo(hostInfo5);
        TdsqlHostInfo tdsqlHostInfo6 = new TdsqlHostInfo(hostInfo6);
        map.put(tdsqlHostInfo1, new NodeMsg(1L, true));
        map.put(tdsqlHostInfo2, new NodeMsg(1L, false));
        map.put(tdsqlHostInfo3, new NodeMsg(2L, false));
        map.put(tdsqlHostInfo4, new NodeMsg(3L, false));
        map.put(tdsqlHostInfo5, new NodeMsg(4L, false));
        for (int i = 0; i < 10; i++) {
            map.incrementAndGet(tdsqlHostInfo1);
        }
        System.out.println(map.get(tdsqlHostInfo1).getCount());

        TdsqlDirectLoadBalanceStrategyFactory instance = TdsqlDirectLoadBalanceStrategyFactory.getInstance();
        TdsqlLoadBalanceStrategy lc = instance.getStrategyInstance("Lc");
        TdsqlHostInfo choice = lc.choice(map);
        System.out.println(choice.getHost());
        System.out.println("null:" + map.remove(tdsqlHostInfo6));

    }

}