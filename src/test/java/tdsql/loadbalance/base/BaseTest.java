package tdsql.loadbalance.base;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_INITIALSIZE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MAXACTIVE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_MINIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_PASSWORD;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONBORROW;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTONRETURN;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_TESTWHILEIDLE;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_USERNAME;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_VALIDATIONQUERY;
import static com.alibaba.druid.pool.DruidDataSourceFactory.createDataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlDataSource;
import com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlXADataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import tdsql.loadbalance.MultiDataSourcesHATest;
import testsuite.util.InstanceInfo;
import testsuite.util.InstanceOp;
import testsuite.util.Undo;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class BaseTest {

    protected static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    protected static final String PROXY_1 = "9.30.2.116:15018";
    protected static final String PROXY_2 = "9.30.2.89:15018";
    protected static final String PROXY_3 = "9.30.2.94:15018";

    protected static final String IDC_USER = "root";

    protected static final String IDC_PASS = "Azaqpvrk#ov#10391356";

    protected static final String[] PROXY_ARRAY = {PROXY_1, PROXY_2, PROXY_3};
    protected static final String DB_MYSQL = "test";
    protected static final String LB_URL_PROPS = "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&logger=Slf4JLogger"
            + "&tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";
    protected static final String LB_URL =
            "jdbc:mysql:loadbalance://" + PROXY_1 + "," + PROXY_2 + "," + PROXY_3 + "/"
                    + DB_MYSQL + LB_URL_PROPS;
    protected static final String USER = "qt4s";
    protected static final String USER_RO = "";
    protected static final String PASS = "g<m:7KNDF.L1<^1C";

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.printf("Running test: %s, method: %s%n",
                testInfo.getTestClass().orElse(Class.forName("java.lang.NullPointerException")).getName(),
                testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        Class.forName(DRIVER_CLASS_NAME);
    }

    protected InstanceInfo produceInstanceInfo() {
        String[] proxyIpList = new String[PROXY_ARRAY.length];
        int[] proxyPortList = new int[PROXY_ARRAY.length];
        for (int i = 0; i < PROXY_ARRAY.length; i++) {
            String[] ipPort = PROXY_ARRAY[i].split(":");
            proxyIpList[i] = ipPort[0];
            proxyPortList[i] = Integer.parseInt(ipPort[1]);
        }
        return new InstanceInfo(proxyIpList, proxyPortList);
    }

    /**
     * 因为在开启异步检测线程的时候可能导致proxy有额外线程连接，因此可以不计算total connection num
     *
     * @param connNum
     */
    protected void validateLoadbalance(List<Integer> connNum) {
        int min, max;
        min = -1;
        max = -1;
        for (Integer num : connNum) {
            if (min == -1)
                min = num;
            else if (min > num)
                min = num;

            if (max == -1)
                max = num;
            else if (max < num) {
                max = num;
            }
        }

        if (max - min > 2) {
            Assertions.fail("the connection algorithm is not balance! connection num" + connNum);
        }
    }

    protected void validateLoadbalance(List<Integer> connNum, int totalConn) {
        int min, max, sum = 0;
        min = -1;
        max = -1;
        for (Integer num : connNum) {
            sum += num;
            if (min == -1)
                min = num;
            else if (min > num)
                min = num;

            if (max == -1)
                max = num;
            else if (max < num) {
                max = num;
            }
        }

        Assertions.assertEquals(totalConn, sum, "total connection number is not equal");
        if (max - min > 2) {
            Assertions.fail("the connection algorithm is not balance! connection num" + connNum);
        }
    }

    protected DataSource createMysqlDataSource() {
        MysqlDataSource ds = new MysqlDataSource();
                                    ds.setURL(LB_URL);
        ds.setUser(USER);
        ds.setPassword(PASS);
        return ds;
    }

    protected DataSource createHikariDataSource(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_CLASS_NAME);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    protected DataSource createHikariDataSource(String jdbcUrl) {
        return this.createHikariDataSource(jdbcUrl, USER, PASS);
    }

    protected DataSource createHikariDataSource() {
        return this.createHikariDataSource(LB_URL);
    }

    protected DataSource createDruidDataSource(String jdbcUrl, String username, String password) throws Exception {
        Properties prop = new Properties();
        prop.setProperty(PROP_DRIVERCLASSNAME, DRIVER_CLASS_NAME);
        prop.setProperty(PROP_URL, jdbcUrl);
        prop.setProperty(PROP_USERNAME, username);
        prop.setProperty(PROP_PASSWORD, password);
        prop.setProperty(PROP_INITIALSIZE, "10");
        prop.setProperty(PROP_MINIDLE, "10");
        prop.setProperty(PROP_MAXACTIVE, "10");
        prop.setProperty(PROP_TESTONBORROW, "false");
        prop.setProperty(PROP_TESTONRETURN, "false");
        prop.setProperty(PROP_TESTWHILEIDLE, "true");
        prop.setProperty(PROP_VALIDATIONQUERY, "select 1");
        return createDataSource(prop);
    }

    protected DataSource createDruidDataSource(String jdbcUrl) throws Exception {
        return this.createDruidDataSource(jdbcUrl, USER ,PASS);
    }

    protected DataSource createDruidDataSource(Properties prop) throws Exception {
        return createDataSource(prop);
    }

    protected DataSource createDruidDataSource() throws Exception {
        return this.createDruidDataSource(LB_URL);
    }

    protected ThreadPoolExecutor initThreadPool(int coreSize, int maxSize) {
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

    protected DruidDataSource initDruidDataSource(String url) throws SQLException {
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
        ds.setMaxWait(5000);
        ds.init();
        return ds;
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName, String jdbcUrl, String username,
            String password) throws SQLException {
        MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
        mysqlXADataSource.setUrl(jdbcUrl);
        mysqlXADataSource.setUser(username);
        mysqlXADataSource.setPassword(password);

        AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
        ds.setXaProperties(new Properties());
        ds.setXaDataSource(mysqlXADataSource);
        ds.setUniqueResourceName(uniqueResourceName);
        ds.setXaDataSourceClassName("com.tencentcloud.tdsql.mysql.cj.jdbc.MysqlXADataSource");
        ds.setMinPoolSize(10);
        ds.setMaxPoolSize(10);
        ds.setMaxLifetime(20000);
        ds.setBorrowConnectionTimeout(10000);
        ds.setLoginTimeout(3);
        ds.setMaintenanceInterval(100);
        ds.setMaxIdleTime(300);
        ds.setTestQuery("select 1");
        return ds;
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName, String jdbcUrl) throws SQLException {
        return this.createAtomikosDataSource(uniqueResourceName, jdbcUrl, USER, PASS);
    }

    protected DataSource createAtomikosDataSource(String uniqueResourceName) throws SQLException {
        return this.createAtomikosDataSource(uniqueResourceName, LB_URL);
    }

    protected DataSource createAtomikosDataSource() throws SQLException {
        return this.createAtomikosDataSource("ds1");
    }

    protected List<QueryTask> createQueryTasks(DataSource dataSource, String datasourceName, ThreadPoolExecutor executor, int taskCnt) throws InterruptedException {
        List<QueryTask> queryTasks = new ArrayList<>();
        for (int i = 0; i < taskCnt; i++) {
            TimeUnit.MILLISECONDS.sleep(100);
            QueryTask queryTask = new QueryTask("query-" + datasourceName + "-" + i, datasourceName, "select 1", dataSource);
            queryTasks.add(queryTask);
            executor.execute( queryTask);
        }
        return queryTasks;
    }

    protected boolean validateQueryTask(List<QueryTask> queryTasks) {
        AtomicBoolean beginStatus = new AtomicBoolean(true);
        queryTasks.forEach(v -> {
            if (!v.getStatus()) {
                beginStatus.set(false);
                System.out.println("Task: " + v.getTaskName() + ", datasource: " + v.getDsName() + " is in faileed status!");
            }
        });
        return beginStatus.get();
    }

    protected void recoverAllProxy(InstanceInfo instanceInfo) throws IOException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            instanceOp.recoverPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS);
        }
    }

    protected Undo faioverOneProxy(InstanceInfo instanceInfo) throws IOException {
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        String failIp = instanceInfo.getProxyIpList()[0];
        System.out.println("close ip port: " + failIp + ":" + instanceInfo.getProxyPort(failIp));
        return instanceOp.setPortFailed(failIp, instanceInfo.getProxyPort(failIp), IDC_USER, IDC_PASS);
    }

    protected List<Undo> failoverAllProxy(InstanceInfo instanceInfo) throws IOException {
        List<Undo> undoList = new ArrayList<>();
        InstanceOp instanceOp = new InstanceOp(instanceInfo);
        for (String ip : instanceInfo.getProxyIpList()) {
            System.out.println("close ip port: " + ip + ":" + instanceInfo.getProxyPort(ip));
            undoList.add(instanceOp.setPortFailed(ip, instanceInfo.getProxyPort(ip), IDC_USER, IDC_PASS));
        }
        return undoList;
    }

    protected void warmUp(DataSource dataSource, int threadCnt, int taskCnt, int waitSec) {
        try {
            ExecutorService pool = Executors.newFixedThreadPool(threadCnt);
            CountDownLatch latch = new CountDownLatch(taskCnt);
            for (int i = 0; i < taskCnt; i++) {
                pool.execute(() -> {
                    try (Connection conn = dataSource.getConnection();
                            Statement stmt = conn.createStatement()) {
                        stmt.executeQuery("show processlist;");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            boolean await = latch.await(waitSec, TimeUnit.SECONDS);
            if (!await) {
                pool.shutdownNow();
            } else {
                pool.shutdown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected static class QueryTask implements Runnable {
        public String getTaskName() {
            return taskName;
        }

        public String getDsName() {
            return dsName;
        }

        private String taskName;
        private String dsName;
        private String querySQL;

        private DataSource dataSource;

        public boolean getStatus() {
            return status;
        }

        private boolean status;

        public QueryTask(String taskName, String dsName, String querySQL, DataSource dataSource) {
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
