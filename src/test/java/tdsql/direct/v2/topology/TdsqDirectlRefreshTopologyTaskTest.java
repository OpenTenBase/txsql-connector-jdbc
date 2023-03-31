package tdsql.direct.v2.topology;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandlerImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectRefreshTopologyTask;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 刷新拓扑任务单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqDirectlRefreshTopologyTaskTest extends TdsqlDirectBaseTest {

    /**
     * 成功 - 读写模式
     */
    @Test
    public void testCase01() {
        TdsqlDirectDataSourceConfig dataSourceConfig = new TdsqlDirectDataSourceConfig(super.defaultDataSourceUuid);
        dataSourceConfig.validateConnectionProperties(super.defaultConnectionUrlRw);
        TdsqlDirectTopologyServer topologyServer = new TdsqlDirectTopologyServer(dataSourceConfig);
        dataSourceConfig.setTopologyServer(topologyServer);
        TdsqlDirectFailoverHandlerImpl handler = new TdsqlDirectFailoverHandlerImpl(dataSourceConfig);
        dataSourceConfig.setFailoverHandler(handler);
        TdsqlDirectScheduleServer scheduleServer = new TdsqlDirectScheduleServer(dataSourceConfig);
        dataSourceConfig.setScheduleServer(scheduleServer);
        TdsqlDirectCacheServer cacheServer = new TdsqlDirectCacheServer(dataSourceConfig);
        dataSourceConfig.setCacheServer(cacheServer);

        assertDoesNotThrow(() -> {
            List<HostInfo> hostsList = super.defaultConnectionUrlRw.getHostsList();
            List<String> hostPortList = hostsList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList());

            TdsqlDirectRefreshTopologyTask task = new TdsqlDirectRefreshTopologyTask(dataSourceConfig, hostPortList,
                    new HashMap<>());
            task.run();

            TdsqlDirectMasterTopologyInfo masterTopologyInfo = cacheServer.getCachedTopologyInfo()
                    .getMasterTopologyInfo();
            TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();

            // topologyServer
            Assertions.assertTrue(topologyServer.getProxyBlacklist().isEmpty());
            // cacheServer
            Assertions.assertNotNull(masterTopologyInfo);
            Assertions.assertEquals(3, cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet().size());
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            // scheduleServer
            Assertions.assertNotNull(masterCounter);
            Assertions.assertEquals(0L, masterCounter.getCount().longValue());
            Assertions.assertEquals(0, scheduleServer.getSlaveSet().size());
            Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(dataSourceConfig),
                    masterCounter.getTdsqlHostInfo());
        });
    }

    /**
     * 成功 - 只读模式 - 主库不承接流量
     */
    @Test
    public void testCase02() {
        TdsqlDirectDataSourceConfig dataSourceConfig = new TdsqlDirectDataSourceConfig(super.defaultDataSourceUuid);
        dataSourceConfig.validateConnectionProperties(super.defaultConnectionUrlRo);
        TdsqlDirectTopologyServer topologyServer = new TdsqlDirectTopologyServer(dataSourceConfig);
        dataSourceConfig.setTopologyServer(topologyServer);
        TdsqlDirectFailoverHandlerImpl handler = new TdsqlDirectFailoverHandlerImpl(dataSourceConfig);
        dataSourceConfig.setFailoverHandler(handler);
        TdsqlDirectScheduleServer scheduleServer = new TdsqlDirectScheduleServer(dataSourceConfig);
        dataSourceConfig.setScheduleServer(scheduleServer);
        TdsqlDirectCacheServer cacheServer = new TdsqlDirectCacheServer(dataSourceConfig);
        dataSourceConfig.setCacheServer(cacheServer);

        assertDoesNotThrow(() -> {
            List<HostInfo> hostsList = super.defaultConnectionUrlRw.getHostsList();
            List<String> hostPortList = hostsList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList());

            TdsqlDirectRefreshTopologyTask task = new TdsqlDirectRefreshTopologyTask(dataSourceConfig, hostPortList,
                    new HashMap<>());
            task.run();

            // topologyServer
            Assertions.assertTrue(topologyServer.getProxyBlacklist().isEmpty());
            // cacheServer
            Assertions.assertNotNull(cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());
            Assertions.assertEquals(3, cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet().size());
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            // scheduleServer
            Assertions.assertNull(scheduleServer.getMaster());
            Assertions.assertEquals(3, scheduleServer.getSlaveSet().size());

            int matchCount = 0;
            for (TdsqlDirectConnectionCounter slaveCounter : scheduleServer.getSlaveSet()) {
                for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : cacheServer.getCachedTopologyInfo()
                        .getSlaveTopologyInfoSet()) {
                    if (slaveTopologyInfo.convertToDirectHostInfo(dataSourceConfig)
                            .equals(slaveCounter.getTdsqlHostInfo())) {
                        matchCount++;
                        Assertions.assertEquals(0L, slaveCounter.getCount().longValue());
                        break;
                    }
                }
            }
            Assertions.assertEquals(3, matchCount);
        });
    }

    /**
     * 成功 - 只读模式 - 主库承接流量
     */
    @Test
    public void testCase03() {
        TdsqlDirectDataSourceConfig dataSourceConfig = new TdsqlDirectDataSourceConfig(super.defaultDataSourceUuid);
        dataSourceConfig.validateConnectionProperties(super.defaultConnectionUrlRo);
        dataSourceConfig.setTdsqlDirectMasterCarryOptOfReadOnlyMode(true);
        TdsqlDirectTopologyServer topologyServer = new TdsqlDirectTopologyServer(dataSourceConfig);
        dataSourceConfig.setTopologyServer(topologyServer);
        TdsqlDirectFailoverHandlerImpl handler = new TdsqlDirectFailoverHandlerImpl(dataSourceConfig);
        dataSourceConfig.setFailoverHandler(handler);
        TdsqlDirectScheduleServer scheduleServer = new TdsqlDirectScheduleServer(dataSourceConfig);
        dataSourceConfig.setScheduleServer(scheduleServer);
        TdsqlDirectCacheServer cacheServer = new TdsqlDirectCacheServer(dataSourceConfig);
        dataSourceConfig.setCacheServer(cacheServer);

        assertDoesNotThrow(() -> {
            List<HostInfo> hostsList = super.defaultConnectionUrlRw.getHostsList();
            List<String> hostPortList = hostsList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList());

            TdsqlDirectRefreshTopologyTask task = new TdsqlDirectRefreshTopologyTask(dataSourceConfig, hostPortList,
                    new HashMap<>());
            task.run();

            TdsqlDirectMasterTopologyInfo masterTopologyInfo = cacheServer.getCachedTopologyInfo()
                    .getMasterTopologyInfo();
            TdsqlDirectConnectionCounter masterCounter = scheduleServer.getMaster();

            // topologyServer
            Assertions.assertTrue(topologyServer.getProxyBlacklist().isEmpty());
            // cacheServer
            Assertions.assertNotNull(masterTopologyInfo);
            Assertions.assertEquals(3, cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet().size());
            Assertions.assertTrue(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            // scheduleServer
            Assertions.assertNotNull(masterCounter);
            Assertions.assertEquals(0L, masterCounter.getCount().longValue());
            Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(dataSourceConfig),
                    masterCounter.getTdsqlHostInfo());
            Assertions.assertEquals(3, scheduleServer.getSlaveSet().size());

            int matchCount = 0;
            for (TdsqlDirectConnectionCounter slaveCounter : scheduleServer.getSlaveSet()) {
                for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : cacheServer.getCachedTopologyInfo()
                        .getSlaveTopologyInfoSet()) {
                    if (slaveTopologyInfo.convertToDirectHostInfo(dataSourceConfig)
                            .equals(slaveCounter.getTdsqlHostInfo())) {
                        matchCount++;
                        Assertions.assertEquals(0L, slaveCounter.getCount().longValue());
                        break;
                    }
                }
            }
            Assertions.assertEquals(3, matchCount);
        });
    }

    /**
     * 失败
     */
    @Test
    public void testCase04() {
        Properties prop = new Properties();
        prop.put(PropertyKey.USER.getKeyName(), "qt4s");
        prop.put(PropertyKey.PASSWORD.getKeyName(), "g<m:7KNDF.L1<^1C");
        ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(
                "jdbc:tdsql-mysql:direct://1.1.1.1:1111,2.2.2.2:2222,3.3.3.3:3333,4.4.4.4:4444/qt4s",
                prop);
        String dataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
        TdsqlDirectDataSourceConfig dataSourceConfig = new TdsqlDirectDataSourceConfig(dataSourceUuid);
        dataSourceConfig.validateConnectionProperties(connectionUrl);
        TdsqlDirectTopologyServer topologyServer = new TdsqlDirectTopologyServer(dataSourceConfig);
        dataSourceConfig.setTopologyServer(topologyServer);
        TdsqlDirectScheduleServer scheduleServer = new TdsqlDirectScheduleServer(dataSourceConfig);
        dataSourceConfig.setScheduleServer(scheduleServer);
        TdsqlDirectCacheServer cacheServer = new TdsqlDirectCacheServer(dataSourceConfig);
        dataSourceConfig.setCacheServer(cacheServer);

        Assertions.assertDoesNotThrow(() -> {
            List<HostInfo> hostsList = connectionUrl.getHostsList();
            List<String> hostPortList = hostsList.stream().map(HostInfo::getHostPortPair).collect(Collectors.toList());

            TdsqlDirectRefreshTopologyTask task = new TdsqlDirectRefreshTopologyTask(dataSourceConfig, hostPortList,
                    new HashMap<>());
            task.run();

            // topologyServer
            // 所有均不可用后，返回空
            Assertions.assertEquals(0, topologyServer.getProxyBlacklist().size());
            // cacheServer
            Assertions.assertNull(cacheServer.getCachedTopologyInfo());
            Assertions.assertFalse(cacheServer.getInitialCached());
            Assertions.assertFalse(cacheServer.getSurvived());
            Assertions.assertNull(cacheServer.getLatestCachedTimeMillis());
            Assertions.assertNull(cacheServer.getClusterName());
            // scheduleServer
            Assertions.assertNull(scheduleServer.getMaster());
            Assertions.assertEquals(0, scheduleServer.getSlaveSet().size());
        });
    }
}
