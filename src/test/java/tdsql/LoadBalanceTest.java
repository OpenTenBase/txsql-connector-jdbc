package tdsql;

import com.tencent.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencent.tdsql.mysql.cj.conf.PropertyKey;
import com.tencent.tdsql.mysql.cj.jdbc.ha.GlobalConnectionScheduler;
import com.tencent.tdsql.mysql.cj.jdbc.util.ActiveConnectionCounter;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author dorianzhang@tencent.com
 */
public class LoadBalanceTest {

    private static final String DB_CLASS = "com.tencent.tdsql.mysql.cj.jdbc.Driver";
    private static final String NATIVE_LB_URL = "jdbc:mysql:loadbalance://%s,%s,%s,%s/%s?%s&%s";
    private static final String JMX_LB_URL = "jdbc:mysql:loadbalance://%s,%s,%s,%s/%s?%s&%s&%s";
    private static final String MYSQL_57_3357 = "9.134.209.89:3357";
    private static final String MYSQL_57_3358 = "9.134.209.89:3358";
    private static final String MYSQL_57_3359 = "9.134.209.89:3359";
    private static final String MYSQL_57_3360 = "9.134.209.89:3360";
    private static final String DB_NAME = "jdbc_test_db";
    private static final String DB_CONFIG = "user=root&password=123456";
    private static final String LB_CONFIG = "ha.loadBalanceStrategy=sed&loadBalanceWeightFactor=1,1,1,1";
    private static final String JMX_CONFIG = "loadBalanceConnectionGroup=first&ha.enableJMX=true";
    private static final String URL = String.format(NATIVE_LB_URL, MYSQL_57_3357, MYSQL_57_3358, MYSQL_57_3359,
            MYSQL_57_3360, DB_NAME, DB_CONFIG, LB_CONFIG);
    private static final String JMX_URL = String.format(JMX_LB_URL, MYSQL_57_3357, MYSQL_57_3358, MYSQL_57_3359,
            MYSQL_57_3360, DB_NAME, DB_CONFIG, LB_CONFIG, JMX_CONFIG);

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DB_CLASS);
    }

    @Test
    public void case01() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        int num = 10;
        CountDownLatch count = new CountDownLatch(num);
        List<Connection> connList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < num; i++) {
            executor.execute(() -> {
                try {
                    Connection conn = DriverManager.getConnection(URL);
                    connList.add(conn);
                    conn.setAutoCommit(false);
                    Statement stmt = conn.createStatement();
                    stmt.executeQuery("select * from java8_test");
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    count.countDown();
                }
            });
        }

        try {
            count.await();
//            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            for (Connection conn : connList) {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Test
    public void case02() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1000, 1000,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        int num = 1000000;
        CountDownLatch countDownLatch = new CountDownLatch(num);
        GlobalConnectionScheduler scheduler = GlobalConnectionScheduler
                .getInstance(new ConcurrentHashMap<String, Long>() {{
                    put("1", 0L);
                    put("2", 0L);
                }});
        ActiveConnectionCounter<String> counter = scheduler.getCounter();
        for (int i = 0; i < num; i++) {
            executor.execute(() -> {
                counter.incrementAndGet("1");
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
        Assertions.assertEquals(num, counter.get("1"));
        Assertions.assertEquals(0, counter.get("2"));
    }

    @Test
    public void case03() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        new Thread(new Repeater(latch)).start();
        new Thread(new Repeater(latch)).start();
        new Thread(new Repeater(latch)).start();
        new Thread(new Repeater(latch)).start();
        latch.await();
    }

    @Test
    public void case04() throws Exception {
        final String[] hosts = new String[]{MYSQL_57_3357, MYSQL_57_3358, MYSQL_57_3359, MYSQL_57_3360};
        StringJoiner hostString = new StringJoiner(",");
        for (String host : hosts) {
            hostString.add(host);
        }
        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), "root");
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "123456");
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "nq");
        props.setProperty(PropertyKey.loadBalanceWeightFactor.getKeyName(), "1,1,1,1");
        props.setProperty(PropertyKey.initialTimeout.getKeyName(), "1");
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.loadBalanceBlocklistTimeout.getKeyName(), "1000");

        StringJoiner propString = new StringJoiner("&");
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            propString.add(entry.getKey() + "=" + entry.getValue());
        }

        int min = 8;
        HikariDataSource ds = new HikariDataSource();
        ds.setConnectionTimeout(60 * 1000);
        ds.setMinimumIdle(min);
        ds.setMaximumPoolSize(min);
        ds.setJdbcUrl(ConnectionUrl.Type.LOADBALANCE_CONNECTION.getScheme() + "//" + hostString + "/" + DB_NAME + "?"
                + propString);

        List<Connection> connList = new ArrayList<>();
        for (int i = 0; i < min; i++) {
            connList.add(ds.getConnection());
        }
        for (Connection conn : connList) {
            conn.close();
        }
    }

    private static Connection newConnection() throws SQLException {
        return DriverManager.getConnection(JMX_URL);
    }

    private static void executeSimpleTransaction(Connection c, int conn, int trans) {
        try {
            c.setAutoCommit(false);
            Statement s = c.createStatement();
            s.executeQuery("SELECT SLEEP(1) /* Connection: " + conn + ", transaction: " + trans + " */");
            c.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static class Repeater implements Runnable {

        private final CountDownLatch latch;

        public Repeater(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    try {
                        Connection c = newConnection();
                        for (int j = 0; j < 10; j++) {
                            executeSimpleTransaction(c, i, j);
                            Thread.sleep(Math.round(100 * Math.random()));
                        }
                        c.close();
                        Thread.sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                this.latch.countDown();
            }
        }
    }
}
