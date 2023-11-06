package tdsql.loadbalance;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;
import testsuite.util.InstanceInfo;
import testsuite.util.InstanceOp;
import testsuite.util.Undo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
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
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1&logger=NullLogger";

    private static final String DB_URL2 = "jdbc:tdsql-mysql:loadbalance://" +
            PROXY_1 + "," + PROXY_3 + "/test"
            + "?tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1&logger=NullLogger";

    private InstanceInfo instanceInfo = produceInstanceInfo();

    private DruidDataSource initDataSource(String url) throws SQLException {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(url);
        ds.setUsername(USER);
        ds.setPassword(PASS);
        ds.setDriverClassName(DRIVER_CLASS_NAME);
        ds.setInitialSize(10);
        ds.setMaxActive(10);
        ds.setMinIdle(10);
        ds.setValidationQuery("select 1");
        ds.setTimeBetweenEvictionRunsMillis(30000);
        ds.setTestWhileIdle(true);
        ds.setPhyTimeoutMillis(10000);
        ds.setTestOnBorrow(true);
        ds.setMaxWait(1000);
        ds.init();
        return ds;
    }

    private ThreadPoolExecutor initThreadPool(int coreSize, int maxSize) {
        return new ThreadPoolExecutor(coreSize,
                maxSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new AbortPolicy());
    }

    private static class AbortPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            System.out.println("Task " + r.toString() + " rejected from " + e.toString());
        }
    }

    @Test
    public void TestHAWithSameIPPortAndDifferentProps() throws SQLException, InterruptedException, IOException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }

        DruidDataSource ds1 = initDataSource(DB_URL1);
        DruidDataSource ds2 = initDataSource(DB_URL2);

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

    private static class QueryTask implements Runnable {
        public String getTaskName() {
            return taskName;
        }

        public String getDsName() {
            return dsName;
        }

        private String taskName;
        private String dsName;
        private String querySQL;

        private DruidDataSource dataSource;

        public boolean getStatus() {
            return status;
        }

        private boolean status;

        public QueryTask(String taskName, String dsName, String querySQL, DruidDataSource dataSource) {
            this.taskName = taskName;
            this.dsName = dsName;
            this.querySQL = querySQL;
            this.dataSource = dataSource;
        }

        @Override
        public void run() {
            int i = 0;
            while(true) {
                i++;
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = dataSource.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(querySQL);

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        break;
                    }
                    status = true;
                    System.out.println("TaskName: " + taskName + " has finished task " + i + "th time!");
                } catch (Throwable e) {
                    status = false;
                    System.out.println("TaskName: " + taskName + " occurred exception! datasource name: " + dsName
                            + ", exception: " + e.getMessage());
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
