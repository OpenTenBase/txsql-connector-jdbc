package tdsql.loadbalance;

import com.alibaba.druid.pool.DruidDataSource;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;
import testsuite.util.InstanceInfo;
import testsuite.util.InstanceOp;
import testsuite.util.Undo;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class FailoverTest extends BaseTest {

    private final  String dbName = "test";
    private final String jdbcUrl = "jdbc:tdsql-mysql:loadbalance:" +
            "//" + PROXY_1 + "," + PROXY_2 + "," + PROXY_3 + "/" + dbName +
            "?tdsqlLoadBalanceStrategy=sed" +
            "&logger=Slf4JLogger" +
            "&tdsqlLoadBalanceWeightFactor=1,1,1" +
            "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000" +
            "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
            "&socketTimeout=5000";

    private InstanceInfo instanceInfo = produceInstanceInfo();

    @Test
    public void testOneProxyFailedWithoutHeartbeatMonitor() throws IOException, SQLException, InterruptedException  {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }
        String newJdbcUrl = jdbcUrl + "&tdsqlLoadBalanceHeartbeatMonitorEnable=false";
        List<Connection> connList = getConnection(newJdbcUrl, 15);
        TimeUnit.SECONDS.sleep(10);
        System.out.println("validate connection number in proxies!");
        List<Integer> connNumList = new ArrayList<>();
        for (String ip : instanceInfo.getProxyIpList()) {
            connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
        }
        validateLoadbalance(connNumList);
        System.out.println("PASS");
        String faileIp = instanceInfo.getProxyIpList()[0];
        System.out.println("start fail one proxy, ip:" + faileIp);
        Undo undo = null;
        try {
            undo = instanceOp.setPortFailed(faileIp, instanceInfo.getProxyPort(faileIp), IDC_USER, IDC_PASS);

            TimeUnit.SECONDS.sleep(10);
            System.out.println("validate new connection number in proxies!");
            connList.addAll(getConnection(newJdbcUrl, 6));
            TimeUnit.SECONDS.sleep(5);
            System.out.println("validate connection number in proxies!");
            connNumList.clear();
            for (String ip : instanceInfo.getProxyIpList()) {
                if (ip.equals(faileIp)) {
                    continue;
                }
                connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
            }
            validateLoadbalance(connNumList);
            undo.undo();
            undo = null;
            System.out.println("recover failed proxy!");
            TimeUnit.SECONDS.sleep(10);
            connList.addAll(getConnection(newJdbcUrl, 8));

            TimeUnit.SECONDS.sleep(5);
            System.out.println("validate connection number in proxies!");
            connNumList.clear();
            for (String ip : instanceInfo.getProxyIpList()) {
                connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
            }
            validateLoadbalance(connNumList);
        } finally {
            connList.forEach(v -> {
                try {
                    v.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            if (undo != null) {
                undo.undo();
            }
        }
    }

    @Test
    public void testAllProxyFailedWithoutHeartbeatMonitor() throws IOException, SQLException, InterruptedException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }
        String newJdbcUrl = jdbcUrl + "&tdsqlLoadBalanceHeartbeatMonitorEnable=false";
        DruidDataSource ds1 = initDruidDataSource(newJdbcUrl);
        int threadNum = 5;
        ThreadPoolExecutor executor = initThreadPool(threadNum, threadNum);

        List<Undo> undoList = new ArrayList<>();
        List<BaseTest.QueryTask> queryTasks = new ArrayList<>();
        try {
            for (int i = 0; i < threadNum; i++) {
                TimeUnit.MILLISECONDS.sleep(100);
                BaseTest.QueryTask queryTask = new BaseTest.QueryTask("query-" + i, "ds1", "select 1", ds1);
                queryTasks.add(queryTask);
                executor.execute( queryTask);
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
            undoList.clear();
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
            undoList.forEach(Undo::undo);
            ds1.close();
        }
    }

    /**
     *
     *
     * @throws IOException
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testOneProxyFailed() throws IOException, SQLException, InterruptedException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }

        List<Connection> connList = getConnection(jdbcUrl, 15);
        TimeUnit.SECONDS.sleep(10);
        System.out.println("validate connection number in proxies!");
        List<Integer> connNumList = new ArrayList<>();
        for (String ip : instanceInfo.getProxyIpList()) {
            connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
        }
        validateLoadbalance(connNumList);
        System.out.println("PASS");
        String faileIp = instanceInfo.getProxyIpList()[0];
        System.out.println("start fail one proxy, ip:" + faileIp);
        Undo undo = null;
        try {
            undo = instanceOp.setPortFailed(faileIp, instanceInfo.getProxyPort(faileIp), IDC_USER, IDC_PASS);

            TimeUnit.SECONDS.sleep(10);
            System.out.println("validate new connection number in proxies!");
            connList.addAll(getConnection(jdbcUrl, 6));
            TimeUnit.SECONDS.sleep(5);
            System.out.println("validate connection number in proxies!");
            connNumList.clear();
            for (String ip : instanceInfo.getProxyIpList()) {
                if (ip.equals(faileIp)) {
                    continue;
                }
                connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
            }
            validateLoadbalance(connNumList);
            undo.undo();
            undo = null;
            System.out.println("recover failed proxy!");
            TimeUnit.SECONDS.sleep(10);
            connList.addAll(getConnection(jdbcUrl, 8));

            TimeUnit.SECONDS.sleep(5);
            System.out.println("validate connection number in proxies!");
            connNumList.clear();
            for (String ip : instanceInfo.getProxyIpList()) {
                connNumList.add(instanceInfo.getConNumberOnEacheProxy(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS, USER, PASS, "test"));
            }
            validateLoadbalance(connNumList);
        } finally {
            connList.forEach(v -> {
                try {
                    v.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            if (undo != null) {
                undo.undo();
            }
        }

    }

    @Test
    public void testAllProxyFailed() throws IOException, SQLException, InterruptedException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }

        DruidDataSource ds1 = initDruidDataSource(jdbcUrl);
        int threadNum = 5;
        ThreadPoolExecutor executor = initThreadPool(threadNum, threadNum);

        List<Undo> undoList = new ArrayList<>();
        List<BaseTest.QueryTask> queryTasks = new ArrayList<>();
        try {
            for (int i = 0; i < threadNum; i++) {
                TimeUnit.MILLISECONDS.sleep(100);
                BaseTest.QueryTask queryTask = new BaseTest.QueryTask("query-" + i, "ds1", "select 1", ds1);
                queryTasks.add(queryTask);
                executor.execute( queryTask);
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
            undoList.clear();
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
            undoList.forEach(Undo::undo);
            ds1.close();
        }
    }

    private List<Connection> getConnection(String jdbcUrl, int num) throws SQLException {
        List<Connection> conns = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            Connection conn = null;
            for (int j = 0; j < 3; j++) {
                try {
                    conn = DriverManager.getConnection(jdbcUrl, USER, PASS);
                    if (conn != null) {
                        break;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            conns.add(conn);
        }
        return conns;
    }
}
