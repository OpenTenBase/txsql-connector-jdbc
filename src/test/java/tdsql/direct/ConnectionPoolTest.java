package tdsql.direct;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.base.BaseTest;

public class ConnectionPoolTest extends BaseTest {

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

        ThreadPoolExecutor taskExecutor = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        taskExecutor.prestartAllCoreThreads();

        ScheduledThreadPoolExecutor monitor = new ScheduledThreadPoolExecutor(1);
        monitor.scheduleAtFixedRate(() -> {
            printAllConnection();
            printScheduleQueue();
            System.out.printf("Monitor: " +
                            "PoolSize: %d, CorePoolSize: %d, Active: %d, " +
                            "Completed: %d, Task: %d, Queue: %d, LargestPoolSize: %d, " +
                            "MaximumPoolSize: %d,  KeepAliveTime: %d, isShutdown: %s, isTerminated: %s\n",
                    taskExecutor.getPoolSize(), taskExecutor.getCorePoolSize(),
                    taskExecutor.getActiveCount(),
                    taskExecutor.getCompletedTaskCount(), taskExecutor.getTaskCount(),
                    taskExecutor.getQueue().size(), taskExecutor.getLargestPoolSize(),
                    taskExecutor.getMaximumPoolSize(), taskExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS),
                    taskExecutor.isShutdown(), taskExecutor.isTerminated());
            System.out.println("Hikari pool total = " + ds.getHikariPoolMXBean().getTotalConnections());
        }, 0L, 5L, TimeUnit.SECONDS);

        for (; ; ) {
            TimeUnit.MILLISECONDS.sleep(100);
            taskExecutor.execute(() -> {
                try (Connection conn = ds.getConnection();
                        Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("select 1");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_CLASS_NAME);
        config.setJdbcUrl(URL_RO);
        config.setUsername(USER_RO);
        config.setPassword(PASS_RO);
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setMaxLifetime(30000);
        ds = new HikariDataSource(config);
    }
}
