package tdsql.loadbalance;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;
import testsuite.util.InstanceInfo;
import testsuite.util.InstanceOp;
import testsuite.util.Undo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiDataSourcesHATest extends BaseTest {
    private static final String DB_URL1 = "jdbc:tdsql-mysql:loadbalance://" +
            PROXY_1 + "," + PROXY_3 + "/test"
            + "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&logger=Slf4JLogger"
            + "&tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";

    private static final String DB_URL2 = "jdbc:tdsql-mysql:loadbalance://" +
            PROXY_1 + "," + PROXY_3 + "/test"
            + "?tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";

    private InstanceInfo instanceInfo = produceInstanceInfo();

    @Test
    public void TestHAWithSameIPPortAndDifferentProps() throws SQLException, InterruptedException, IOException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }

        DruidDataSource ds1 = initDruidDataSource(DB_URL1);
        DruidDataSource ds2 = initDruidDataSource(DB_URL2);

        int threadNum = 5;
        ThreadPoolExecutor executor = initThreadPool(threadNum, threadNum);

        List<Undo> undoList = new ArrayList<>();
        List<QueryTask> queryTasks = new ArrayList<>();
        try {
            for (int i = 0; i < threadNum; i++) {
                TimeUnit.MILLISECONDS.sleep(100);
                if (i % 2 == 0) {
                    QueryTask queryTask = new QueryTask("query-" + i, "ds1", "select 1", ds1);
                    queryTasks.add(queryTask);
                    executor.execute( queryTask);
                } else {
                    QueryTask queryTask = new QueryTask("query-" + i, "ds2", "select 1", ds2);
                    queryTasks.add(queryTask);
                    executor.execute(queryTask);
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            AtomicBoolean beginStatus = new AtomicBoolean(true);
            queryTasks.forEach(v -> {
                if (!v.getStatus()) {
                    beginStatus.set(false);
                    System.out.println("Task: " + v.getTaskName() + ", datasource: " + v.getDsName() + " is in faileed status!");
                }
            });
            if (!beginStatus.get()) {
                throw new RuntimeException("Not all query tasks is in successful status!");
            }

            for (String ip : instanceInfo.getProxyIpList()) {
                System.out.println("close ip port: " + ip + ":" + instanceInfo.getProxyPort(ip));
                undoList.add(instanceOp.setPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS));
            }
            try {
                TimeUnit.SECONDS.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("recover all ip port: ");
            undoList.forEach(v -> {v.undo();});
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            AtomicBoolean finalStatus = new AtomicBoolean(true);
            queryTasks.forEach(v -> {
                if (!v.getStatus()) {
                    finalStatus.set(false);
                    System.out.println("Task: " + v.getTaskName() + ", datasource: " + v.getDsName() + " has not recovered!");
                }
            });
            if (!finalStatus.get()) {
                throw new RuntimeException("Not all query tasks recoverd from failed!");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            executor.shutdownNow();
            ds1.close();
            ds2.close();
        }
    }
}
