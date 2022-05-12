package tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
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
import java.util.concurrent.ThreadLocalRandom;
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

        ScheduledThreadPoolExecutor topoExecutor = new ScheduledThreadPoolExecutor(1);
        topoExecutor.scheduleWithFixedDelay(() -> {
            try {
                TimeUnit.SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, 5));
                System.out.println("=========================================================> Topo Refreshing ......");
                TdsqlDirectTopoServer.getInstance().refreshTopology(false);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 10L, 1L, TimeUnit.SECONDS);

        ScheduledThreadPoolExecutor monitor = new ScheduledThreadPoolExecutor(1);
        monitor.scheduleAtFixedRate(() -> {
//            printAllConnection();
//            printScheduleQueue();
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
        int min = 20;
        HikariConfig config = new HikariConfig();
        config.setUsername("root");
        config.setPassword("123456");
        config.setMinimumIdle(min);
        config.setMaximumPoolSize(min);
        config.setMaxLifetime(30000);
        config.setJdbcUrl("jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db"
                + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=10000");
        ds = new HikariDataSource(config);
    }
}
