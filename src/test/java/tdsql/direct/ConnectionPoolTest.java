package tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode.RW;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.direct.base.BaseTest;

public class ConnectionPoolTest extends BaseTest {

    @Test
    public void testInReadOnly() throws InterruptedException {
        HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource(10, 10, RO);
        testHikariPool(hikariDataSource);
    }

    @Test
    public void testInReadWrite() throws InterruptedException {
        HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource(10, 10, RW);
        testHikariPool(hikariDataSource);
    }

    private void testHikariPool(HikariDataSource hikariDataSource) throws InterruptedException {
        hikariDataSource.setMaxLifetime(30000);

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
            System.out.println("Hikari pool total = " + hikariDataSource.getHikariPoolMXBean().getTotalConnections());
        }, 0L, 5L, TimeUnit.SECONDS);

        for (; ; ) {
            TimeUnit.MILLISECONDS.sleep(100);
            taskExecutor.execute(() -> {
                try (Connection conn = hikariDataSource.getConnection();
                        Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("select 1");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
