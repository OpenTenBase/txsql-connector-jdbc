package tdsql.direct.v2.strategy;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyEnum.LC;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyEnum.SED;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.TdsqlLoadBalanceStrategyFactory;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 负载均衡策略算法单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlAlgorithmTest extends TdsqlDirectBaseTest {

    private TdsqlDirectDataSourceConfig dataSourceConfig;
    private TdsqlDirectScheduleServer scheduleServer;
    private TdsqlLoadBalanceStrategy<TdsqlDirectConnectionCounter> algorithm;

    @BeforeEach
    public void beforeEach() {
        this.dataSourceConfig = new TdsqlDirectDataSourceConfig(super.defaultDataSourceUuid);
        this.dataSourceConfig.setConnectionUrl(super.defaultConnectionUrlRo);
        this.scheduleServer = new TdsqlDirectScheduleServer(this.dataSourceConfig);
    }

    /**
     * LC - 单线程
     */
    @Test
    public void testCase01() {
        this.dataSourceConfig.setTdsqlLoadBalanceStrategy(LC);
        String[] slavesStr = {"2.2.2.2:2222@100@0@0", "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0",
                "5.5.5.5:5555@50@0@0"};
        for (TdsqlDirectSlaveTopologyInfo slaveTopoInfo : super.newSlaveTopologyInfoSet(slavesStr)) {
            scheduleServer.addSlave(slaveTopoInfo.convertToDirectHostInfo(this.dataSourceConfig));
        }
        this.algorithm = TdsqlLoadBalanceStrategyFactory.getInstance(LC);

        int loopCount = 10000;
        for (int i = 0; i < loopCount; i++) {
            this.algorithm.choice(Collections.unmodifiableSet(this.scheduleServer.getSlaveSet()));
        }

        long hostCount = loopCount / slavesStr.length;
        for (TdsqlDirectConnectionCounter slaveCounter : this.scheduleServer.getSlaveSet()) {
            Assertions.assertEquals(hostCount, slaveCounter.getCount().longValue());
        }
    }

    /**
     * LC - 多线程
     */
    @Test
    public void testCase02() {
        this.dataSourceConfig.setTdsqlLoadBalanceStrategy(LC);
        String[] slavesStr = {"2.2.2.2:2222@100@0@0", "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0",
                "5.5.5.5:5555@50@0@0"};
        for (TdsqlDirectSlaveTopologyInfo slaveTopoInfo : super.newSlaveTopologyInfoSet(slavesStr)) {
            scheduleServer.addSlave(slaveTopoInfo.convertToDirectHostInfo(this.dataSourceConfig));
        }
        this.algorithm = TdsqlLoadBalanceStrategyFactory.getInstance(LC);

        int loopCount = 50000000;
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        CountDownLatch latch = new CountDownLatch(loopCount);
        for (int i = 0; i < loopCount; i++) {
            executorService.execute(() -> {
                try {
                    this.algorithm.choice(Collections.unmodifiableSet(this.scheduleServer.getSlaveSet()));
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        } finally {
            executorService.shutdownNow();
        }

        long hostCount = loopCount / slavesStr.length;
        for (TdsqlDirectConnectionCounter slaveCounter : this.scheduleServer.getSlaveSet()) {
            Assertions.assertEquals(hostCount, slaveCounter.getCount().longValue());
        }
    }

    /**
     * SED - 单线程
     */
    @Test
    public void testCase03() {
        this.dataSourceConfig.setTdsqlLoadBalanceStrategy(SED);
        String[] slavesStr = {"2.2.2.2:2222@100@0@0", "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@100@0@0",
                "5.5.5.5:5555@100@0@0", "6.6.6.6:6666@50@0@0", "7.7.7.7:7777@50@0@0"};
        for (TdsqlDirectSlaveTopologyInfo slaveTopoInfo : super.newSlaveTopologyInfoSet(slavesStr)) {
            scheduleServer.addSlave(slaveTopoInfo.convertToDirectHostInfo(this.dataSourceConfig));
        }
        this.algorithm = TdsqlLoadBalanceStrategyFactory.getInstance(SED);

        int loopCount = 10000;
        for (int i = 0; i < loopCount; i++) {
            this.algorithm.choice(Collections.unmodifiableSet(this.scheduleServer.getSlaveSet()));
        }

        // 权重100计算为2个
        long hostCount = loopCount / (2 + 2 + 2 + 2 + 1 + 1);
        for (TdsqlDirectConnectionCounter slaveCounter : this.scheduleServer.getSlaveSet()) {
            if ("6.6.6.6:6666".equalsIgnoreCase(slaveCounter.getTdsqlHostInfo().getHostPortPair())
                    || "7.7.7.7:7777".equalsIgnoreCase(slaveCounter.getTdsqlHostInfo().getHostPortPair())) {
                Assertions.assertEquals(hostCount, slaveCounter.getCount().longValue());
            } else {
                Assertions.assertEquals(hostCount * 2, slaveCounter.getCount().longValue());
            }
        }
    }

    /**
     * SED - 多线程
     */
    @Test
    public void testCase04() {
        this.dataSourceConfig.setTdsqlLoadBalanceStrategy(SED);
        String[] slavesStr = {"2.2.2.2:2222@100@0@0", "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@100@0@0",
                "5.5.5.5:5555@100@0@0", "6.6.6.6:6666@50@0@0", "7.7.7.7:7777@50@0@0"};
        for (TdsqlDirectSlaveTopologyInfo slaveTopoInfo : super.newSlaveTopologyInfoSet(slavesStr)) {
            scheduleServer.addSlave(slaveTopoInfo.convertToDirectHostInfo(this.dataSourceConfig));
        }
        this.algorithm = TdsqlLoadBalanceStrategyFactory.getInstance(SED);

        int loopCount = 50000000;
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        CountDownLatch latch = new CountDownLatch(loopCount);
        for (int i = 0; i < loopCount; i++) {
            executorService.execute(() -> {
                try {
                    this.algorithm.choice(Collections.unmodifiableSet(this.scheduleServer.getSlaveSet()));
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        } finally {
            executorService.shutdownNow();
        }

        // 权重100计算为2个
        long hostCount = loopCount / (2 + 2 + 2 + 2 + 1 + 1);
        for (TdsqlDirectConnectionCounter slaveCounter : this.scheduleServer.getSlaveSet()) {
            if ("6.6.6.6:6666".equalsIgnoreCase(slaveCounter.getTdsqlHostInfo().getHostPortPair())
                    || "7.7.7.7:7777".equalsIgnoreCase(slaveCounter.getTdsqlHostInfo().getHostPortPair())) {
                Assertions.assertEquals(hostCount, slaveCounter.getCount().longValue());
            } else {
                Assertions.assertEquals(hostCount * 2, slaveCounter.getCount().longValue());
            }
        }
    }
}
