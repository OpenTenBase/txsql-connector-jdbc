package tdsql.direct.v2.cache;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RW;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectCacheServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverHandlerImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverMasterHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.failover.TdsqlDirectFailoverSlavesHandler;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.manage.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 缓存服务单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectCacheServerTest extends TdsqlDirectBaseTest {

    private TdsqlDirectDataSourceConfig dataSourceConfig;
    private TdsqlDirectScheduleServer scheduleServer;
    private TdsqlDirectFailoverHandler failoverHandler;
    private TdsqlDirectCacheServer cacheServer;

    @BeforeEach
    public void beforeEach() {
        this.init(DEFAULT_RW_MODE);
    }

    /**
     * 检查缓存服务初始化后的状态
     */
    @Test
    public void testCase01() {
        Assertions.assertEquals(this.dataSourceConfig, this.cacheServer.getDataSourceConfig());
        Assertions.assertEquals(super.defaultDataSourceUuid, this.cacheServer.getDataSourceUuid());
        Assertions.assertNotNull(this.cacheServer.getCacheComparator());
        Assertions.assertEquals(this.scheduleServer, this.cacheServer.getScheduleServer());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertFalse(this.cacheServer.getInitialCached());
        Assertions.assertNull(this.cacheServer.getClusterName());
        Assertions.assertNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNull(this.cacheServer.getLatestCachedTimeMillis());
    }

    /**
     * 读写模式
     */
    @Test
    public void testCase02() {
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertNotEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(0, matchCount);
    }

    /**
     * 只读模式，主库不承接流量
     */
    @Test
    public void testCase03() {
        this.init(RO);

        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        Assertions.assertNull(this.scheduleServer.getMaster());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 只读模式，主库承接流量
     */
    @Test
    public void testCase04() {
        this.init(RO, true);

        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 异常判断
     */
    @Test
    public void testCase05() {
        // 拓扑信息为空不允许缓存
        Assertions.assertThrows(TdsqlDirectCacheTopologyException.class, () -> this.cacheServer.compareAndCache(null));

        // UUID不同不允许缓存
        Assertions.assertThrows(TdsqlDirectCacheTopologyException.class, () -> {
            this.cacheServer.compareAndCache(super.newTopologyInfo());
            this.cacheServer.compareAndCache(super.newTopologyInfo(UUID.randomUUID().toString(), DEFAULT_CLUSTER_NAME));
        });

        // 集群名称不同不允许缓存
        Assertions.assertThrows(TdsqlDirectCacheTopologyException.class, () -> {
            this.cacheServer.compareAndCache(super.newTopologyInfo());
            this.cacheServer.compareAndCache(super.newTopologyInfo(defaultDataSourceUuid, "different-cluster-name"));
        });
    }

    /**
     * 打印信息
     */
    @Test
    public void testCase06() {
        this.cacheServer.compareAndCache(super.newTopologyInfo());
        Assertions.assertEquals(
                "[Master:1.1.1.1:1111],[Slave:2.2.2.2:2222,weight:100,delay:0],[Slave:3.3.3.3:3333,weight:100,delay:0],[Slave:4.4.4.4:4444,weight:50,delay:0]",
                this.cacheServer.getCachedTopologyInfo().printPretty());
    }

    /**
     * 读写模式 - 主库变化
     */
    @Test
    public void testCase07() {
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 第二次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet()));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertNotEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(0, matchCount);
    }

    /**
     * 读写模式 - 从库变化
     */
    @Test
    public void testCase08() {
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 第二次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo(),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertNotEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(0, matchCount);
    }

    /**
     * 读写模式 - 主库、从库同时变化
     */
    @Test
    public void testCase09() {
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 第二次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertNotEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(0, matchCount);
    }

    /**
     * 只读模式 - 主库变化 - 主库不承接流量
     */
    @Test
    public void testCase10() {
        this.init(RO);

        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 第二次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet()));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        Assertions.assertNull(this.scheduleServer.getMaster());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 只读模式 - 主库变化 - 主库承接流量
     */
    @Test
    public void testCase11() {
        this.init(RO, true);

        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        // 第二次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet()));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("2.2.2.2:2222@100@0@0",
                "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(slaveTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : slaveTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 只读模式 - 从库变化 - 主库不承接流量
     */
    @Test
    public void testCase12() {
        this.init(RO);
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo(),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        Assertions.assertNull(this.scheduleServer.getMaster());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectSlaveTopologyInfo> scheduleTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@0", "5.5.5.5:5555@100@0@0");
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(scheduleTopologyInfoSet.size(), slaveCounterSet.size());
        // 因为没有设置最大超时时间，所以忽略了延迟的改变
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : scheduleTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 只读模式 - 从库变化 - 主库承接流量
     */
    @Test
    public void testCase13() {
        this.init(RO, true);
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo(),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("1.1.1.1:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectSlaveTopologyInfo> scheduleTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@0", "5.5.5.5:5555@100@0@0");
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        Assertions.assertEquals(scheduleTopologyInfoSet.size(), slaveCounterSet.size());
        // 因为没有设置最大超时时间，所以忽略了延迟的改变
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : scheduleTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(3, matchCount);
    }

    /**
     * 只读模式 - 主库、从库都变化 - 主库不承接流量 - 设置超时时间
     */
    @Test
    public void testCase14() {
        this.init(RO);
        this.dataSourceConfig.setTdsqlDirectMaxSlaveDelaySeconds(8);
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        Assertions.assertNull(this.scheduleServer.getMaster());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectSlaveTopologyInfo> scheduleTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "5.5.5.5:5555@100@0@0");
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        // 因为设置了超时时间，应该只有两个从库ip存在
        Assertions.assertEquals(scheduleTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : scheduleTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(2, matchCount);
    }

    /**
     * 只读模式 - 主库、从库都变化 - 主库承接流量 - 设置超时时间
     */
    @Test
    public void testCase15() {
        this.init(RO, true);
        this.dataSourceConfig.setTdsqlDirectMaxSlaveDelaySeconds(8);
        // 第一次缓存
        this.cacheServer.compareAndCache(super.newTopologyInfo());

        Assertions.assertEquals(DEFAULT_CLUSTER_NAME, this.cacheServer.getClusterName());
        Assertions.assertNotNull(this.cacheServer.getCachedTopologyInfo());
        Assertions.assertNotNull(this.cacheServer.getLatestCachedTimeMillis());
        Assertions.assertFalse(this.cacheServer.getSurvived());
        Assertions.assertTrue(this.cacheServer.getInitialCached());

        this.cacheServer.compareAndCache(super.newTopologyInfo(super.newMasterTopologyInfo("11.11.11.11:1111@0@0"),
                super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0", "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0")));

        // 缓存的主库信息
        TdsqlDirectMasterTopologyInfo masterTopologyInfo = super.newMasterTopologyInfo("11.11.11.11:1111@0@0");
        Assertions.assertEquals(masterTopologyInfo, this.cacheServer.getCachedTopologyInfo().getMasterTopologyInfo());

        // 缓存的备库信息
        Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "4.4.4.4:4444@50@0@10", "5.5.5.5:5555@100@0@0");
        Assertions.assertEquals(slaveTopologyInfoSet,
                this.cacheServer.getCachedTopologyInfo().getSlaveTopologyInfoSet());

        // 间接加入到调度服务的主库信息
        TdsqlDirectConnectionCounter masterCounter = this.scheduleServer.getMaster();
        Assertions.assertEquals(masterTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig),
                masterCounter.getTdsqlHostInfo());
        Assertions.assertEquals(0L, masterCounter.getCount().intValue());

        // 间接加入到调度服务的备库信息
        int matchCount = 0;
        Set<TdsqlDirectSlaveTopologyInfo> scheduleTopologyInfoSet = super.newSlaveTopologyInfoSet("3.3.3.3:3333@50@0@0",
                "5.5.5.5:5555@100@0@0");
        Set<TdsqlDirectConnectionCounter> slaveCounterSet = this.scheduleServer.getSlaveSet();
        // 因为设置了超时时间，应该只有两个从库存在
        Assertions.assertEquals(scheduleTopologyInfoSet.size(), slaveCounterSet.size());
        for (TdsqlDirectSlaveTopologyInfo slaveTopologyInfo : scheduleTopologyInfoSet) {
            TdsqlDirectHostInfo directHostInfo = slaveTopologyInfo.convertToDirectHostInfo(this.dataSourceConfig);
            for (TdsqlDirectConnectionCounter counter : slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(directHostInfo)) {
                    matchCount++;
                    break;
                }
            }
        }
        Assertions.assertEquals(2, matchCount);
    }

    private void init(TdsqlDirectReadWriteModeEnum rwMode) {
        this.init(rwMode, false);
    }

    private void init(TdsqlDirectReadWriteModeEnum rwMode, Boolean masterAsSlave) {
        this.dataSourceConfig = new TdsqlDirectDataSourceConfig(defaultDataSourceUuid);
        if (RW.equals(rwMode)) {
            this.dataSourceConfig.validateConnectionProperties(super.defaultConnectionUrlRw);
        } else {
            this.dataSourceConfig.validateConnectionProperties(super.defaultConnectionUrlRo);
        }
        this.dataSourceConfig.setTdsqlDirectMasterCarryOptOfReadOnlyMode(masterAsSlave);

        this.scheduleServer = new TdsqlDirectScheduleServer(this.dataSourceConfig);
        this.dataSourceConfig.setScheduleServer(this.scheduleServer);

        TdsqlDirectConnectionManager connectionManager = new TdsqlDirectConnectionManager(this.dataSourceConfig);
        this.dataSourceConfig.setConnectionManager(connectionManager);
        this.failoverHandler = new TdsqlDirectFailoverHandlerImpl(this.dataSourceConfig);
        this.dataSourceConfig.setFailoverHandler(failoverHandler);

        this.cacheServer = new TdsqlDirectCacheServer(this.dataSourceConfig);
    }
}
