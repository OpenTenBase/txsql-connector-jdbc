package tdsql.direct.v2.schedule;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RW;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectScheduleTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 调度服务单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectScheduleServerTest extends TdsqlDirectBaseTest {

    private TdsqlDirectDataSourceConfig dataSourceConfig;
    private TdsqlDirectScheduleServer scheduleServer;

    @BeforeEach
    public void beforeEach() {
        this.dataSourceConfig = new TdsqlDirectDataSourceConfig(defaultDataSourceUuid);
        this.dataSourceConfig.setTdsqlDirectReadWriteMode(RW);
        this.dataSourceConfig.setTdsqlDirectMasterCarryOptOfReadOnlyMode(true);
        this.scheduleServer = new TdsqlDirectScheduleServer(this.dataSourceConfig);
    }

    /**
     * 不允许加入空的主库信息
     */
    @Test
    public void testCase01() {
        TdsqlDirectHostInfo emptyMaster = TdsqlDirectHostInfo.empty();

        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.addMaster(emptyMaster));
    }

    /**
     * 不允许加入不同数据源的主库信息
     */
    @Test
    public void testCase02() {
        String differentDataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(
                ConnectionUrl.getConnectionUrlInstance("jdbc:tdsql-mysql:direct://1.1.1.1:1111/test",
                        defaultProperties));
        TdsqlDirectDataSourceConfig differentDataSourceConfig = new TdsqlDirectDataSourceConfig(
                differentDataSourceUuid);
        HostInfo randomHostInfo = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        TdsqlDirectHostInfo master = new TdsqlDirectHostInfo(differentDataSourceConfig, randomHostInfo, 100, 0);

        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.addMaster(master));
    }

    /**
     * 主库加入调度服务
     */
    @Test
    public void testCase03() {
        HostInfo hostInfo = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo master1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo, 100, 0);
        TdsqlDirectHostInfo master2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo, 100, 0);

        // 主库信息，第一次加入调度服务
        this.scheduleServer.addMaster(master1);

        Assertions.assertNotNull(this.scheduleServer.getMaster());
        Assertions.assertEquals(master2, this.scheduleServer.getMaster().getTdsqlHostInfo());
        Assertions.assertEquals(0L, this.scheduleServer.getMaster().getCount().longValue());

        // 主库信息，不允许再次加入
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.addMaster(master1));
    }

    /**
     * 更新主库信息
     */
    @Test
    public void testCase04() {
        HostInfo hostInfo1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo master1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo1, 100, 0);
        TdsqlDirectHostInfo master2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo2, 100, 0);

        // 未加入主库信息前，不允许更新
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateMaster(master1, master2));

        // 主库信息，第一次加入调度服务
        this.scheduleServer.addMaster(master1);

        // 相同的主库信息，不允许更新
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateMaster(master1, master1));

        HostInfo hostInfo3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo master3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo3, 100, 0);

        // 已加入主库信息不相等，不允许更新
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateMaster(master2, master3));

        TdsqlDirectHostInfo emptyMaster = TdsqlDirectHostInfo.empty();

        // 待加入主库信息，不允许为空
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateMaster(master1, emptyMaster));

        // 更新成功
        this.scheduleServer.updateMaster(master1, master3);
    }

    /**
     * 备库加入调度服务
     */
    @Test
    public void testCase05() {
        HostInfo hostInfo1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo slave1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo1, 100, 0, 0);
        TdsqlDirectHostInfo slave2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo2, 100, 0, 0);
        TdsqlDirectHostInfo slave3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo3, 100, 0, 0);

        this.scheduleServer.addSlave(slave1);
        this.scheduleServer.addSlave(slave2);
        this.scheduleServer.addSlave(slave3);

        Assertions.assertNotNull(this.scheduleServer.getSlaveSet());
        Assertions.assertEquals(3, this.scheduleServer.getSlaveSet().size());

        HostInfo hi1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hi2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hi3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo s1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi1, 100, 0, 0);
        TdsqlDirectHostInfo s2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi2, 100, 0, 0);
        TdsqlDirectHostInfo s3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi3, 100, 0, 0);

        TdsqlDirectConnectionCounter c1 = new TdsqlDirectConnectionCounter(s1);
        TdsqlDirectConnectionCounter c2 = new TdsqlDirectConnectionCounter(s2);
        TdsqlDirectConnectionCounter c3 = new TdsqlDirectConnectionCounter(s3);

        int deepEquals = 0;
        for (TdsqlDirectConnectionCounter counter : this.scheduleServer.getSlaveSet()) {
            if (Objects.deepEquals(counter, c1) || Objects.deepEquals(counter, c2) || Objects.deepEquals(counter, c3)) {
                deepEquals++;
            }
            Assertions.assertEquals(0L, counter.getCount().longValue());
        }
        Assertions.assertEquals(3, deepEquals);
    }

    /**
     * 不允许重复添加相同备库
     */
    @Test
    public void testCase06() {
        HostInfo hostInfo1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo slave1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo1, 100, 0, 0);
        TdsqlDirectHostInfo slave2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo2, 100, 0, 0);
        TdsqlDirectHostInfo slave3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo3, 100, 0, 0);

        this.scheduleServer.addSlave(slave1);
        this.scheduleServer.addSlave(slave2);
        this.scheduleServer.addSlave(slave3);

        Assertions.assertNotNull(this.scheduleServer.getSlaveSet());
        Assertions.assertEquals(3, this.scheduleServer.getSlaveSet().size());

        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class, () -> this.scheduleServer.addSlave(slave1));
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class, () -> this.scheduleServer.addSlave(slave2));
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class, () -> this.scheduleServer.addSlave(slave3));
    }

    /**
     * 从调度服务删除备库
     */
    @Test
    public void testCase07() {
        HostInfo hostInfo1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo slave1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo1, 100, 0, 0);
        TdsqlDirectHostInfo slave2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo2, 100, 0, 0);
        TdsqlDirectHostInfo slave3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo3, 100, 0, 0);

        this.scheduleServer.addSlave(slave1);
        this.scheduleServer.addSlave(slave2);
        this.scheduleServer.addSlave(slave3);

        HostInfo hi1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hi2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hi3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hi4 = new HostInfo(defaultMainHost.getOriginalUrl(), "4.4.4.4", 4444, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo s1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi1, 100, 0, 0);
        TdsqlDirectHostInfo s2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi2, 100, 0, 0);
        TdsqlDirectHostInfo s3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi3, 100, 0, 0);
        TdsqlDirectHostInfo s4 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi4, 100, 0, 0);

        // 删除备库
        this.scheduleServer.removeSlave(s1);

        Assertions.assertEquals(2, this.scheduleServer.getSlaveSet().size());

        // 删除备库
        this.scheduleServer.removeSlave(s2);

        Assertions.assertEquals(1, this.scheduleServer.getSlaveSet().size());

        // 重复删除备库
        this.scheduleServer.removeSlave(s2);

        Assertions.assertEquals(1, this.scheduleServer.getSlaveSet().size());

        // 删除备库
        this.scheduleServer.removeSlave(s4);

        Assertions.assertEquals(1, this.scheduleServer.getSlaveSet().size());

        // 判断删除后剩余备库
        TdsqlDirectConnectionCounter c3 = new TdsqlDirectConnectionCounter(s3);
        for (TdsqlDirectConnectionCounter counter : this.scheduleServer.getSlaveSet()) {
            Assertions.assertTrue(Objects.deepEquals(counter, c3));
        }
    }

    /**
     * 更新备库信息
     */
    @Test
    public void testCase08() {
        HostInfo hostInfo1 = new HostInfo(defaultMainHost.getOriginalUrl(), "1.1.1.1", 1111, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());
        HostInfo hostInfo3 = new HostInfo(defaultMainHost.getOriginalUrl(), "3.3.3.3", 3333, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo slave1 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo1, 100, 0, 0);
        TdsqlDirectHostInfo slave2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo2, 100, 0, 0);
        TdsqlDirectHostInfo slave3 = new TdsqlDirectHostInfo(this.dataSourceConfig, hostInfo3, 100, 0, 0);

        this.scheduleServer.addSlave(slave1);

        // 不允许更新相同的备库
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateSlave(slave1, slave1));

        this.scheduleServer.updateSlave(slave1, slave2);

        // 已有备库不存在
        Assertions.assertThrows(TdsqlDirectScheduleTopologyException.class,
                () -> this.scheduleServer.updateSlave(slave1, slave3));

        Assertions.assertNotNull(this.scheduleServer.getSlaveSet());
        Assertions.assertEquals(1, this.scheduleServer.getSlaveSet().size());

        HostInfo hi2 = new HostInfo(defaultMainHost.getOriginalUrl(), "2.2.2.2", 2222, "", "",
                defaultMainHost.getHostProperties());

        TdsqlDirectHostInfo s2 = new TdsqlDirectHostInfo(this.dataSourceConfig, hi2, 100, 0, 0);

        for (TdsqlDirectConnectionCounter counter : this.scheduleServer.getSlaveSet()) {
            if (!counter.getTdsqlHostInfo().equals(s2)) {
                Assertions.fail();
            }
        }
    }
}
