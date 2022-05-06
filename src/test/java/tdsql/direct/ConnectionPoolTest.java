package tdsql.direct;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.base.TdsqlBaseTest;

public class ConnectionPoolTest extends TdsqlBaseTest {

    private static HikariDataSource ds;

    @Test
    public void case01() throws SQLException {
        initDataSource();
        try (Connection conn = ds.getConnection()) {
            printAllConnection();
            printScheduleQueue();
        }
    }

    @Test
    public void case02() throws InterruptedException {
        initDataSource();
        int cnt = 20;
        CountDownLatch latch = new CountDownLatch(cnt);
        ExecutorService executorService = Executors.newFixedThreadPool(cnt);
        for (int i = 0; i < cnt; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try (Connection conn = ds.getConnection()) {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (SQLException | InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        executorService.shutdown();
        latch.await();

        printAllConnection();
        printScheduleQueue();
    }

    @Test
    public void case03() throws InterruptedException {
        case02();
        ds.close();
        initDataSource();
        case02();
    }

    @Test
    public void case04() throws InterruptedException {
        initDataSource();

        ScheduledThreadPoolExecutor monitor = new ScheduledThreadPoolExecutor(1);
        monitor.scheduleAtFixedRate(() -> {
            printAllConnection();
            printScheduleQueue();
            System.out.println("Hikari pool total = " + ds.getHikariPoolMXBean().getTotalConnections());
        }, 5L, 5L, TimeUnit.SECONDS);

        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (; ; ) {
            TimeUnit.MILLISECONDS.sleep((long) Math.floor((Math.random() * 50)));
            executorService.execute(() -> {
                try (Connection conn = ds.getConnection();
                        Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("show processlist;");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void initDataSource() {
        int min = 20;
        ds = new HikariDataSource();
        ds.setUsername("root");
        ds.setPassword("123456");
        ds.setMinimumIdle(min);
        ds.setMaximumPoolSize(min);
        ds.setMaxLifetime(30000);
        ds.setJdbcUrl("jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db"
                + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=10000");
    }
}
