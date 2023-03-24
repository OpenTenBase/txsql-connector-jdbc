package tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlThreadFactoryBuilder;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.direct.base.BaseTest;

public class ConnectionPoolTest extends BaseTest {

    @Test
    public void testInReadOnly() throws InterruptedException {
        HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource(100, 100, RO);
        testHikariPool(hikariDataSource);
    }

    @Test
    public void testInReadWrite() throws InterruptedException {
        HikariDataSource hikariDataSource = (HikariDataSource) createHikariDataSource(100, 100, RW);
        testHikariPool(hikariDataSource);
    }

    private void testHikariPool(HikariDataSource hikariDataSource) throws InterruptedException {
        //        hikariDataSource.setMaxLifetime(30000);

        ThreadPoolExecutor taskExecutor = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new TdsqlThreadFactoryBuilder().setDaemon(true).setNameFormat("Task-pool-%d").build());
        taskExecutor.prestartAllCoreThreads();

        /*ScheduledThreadPoolExecutor monitor = new ScheduledThreadPoolExecutor(1);
        monitor.scheduleAtFixedRate(() -> {
            printAllConnection();
            printScheduleQueue();
            System.out.printf("Monitor: " +
                            "PoolSize: %d, CorePoolSize: %d, ActiveCount: %d, " +
                            "CompletedTaskCount: %d, TaskCount: %d, TaskQueueSize: %d, LargestPoolSize: %d, " +
                            "MaximumPoolSize: %d, KeepAliveTime: %d, IsShutdown: %s, IsTerminated: %s\n",
                    taskExecutor.getPoolSize(), taskExecutor.getCorePoolSize(),
                    taskExecutor.getActiveCount(),
                    taskExecutor.getCompletedTaskCount(), taskExecutor.getTaskCount(),
                    taskExecutor.getQueue().size(), taskExecutor.getLargestPoolSize(),
                    taskExecutor.getMaximumPoolSize(), taskExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS),
                    taskExecutor.isShutdown(), taskExecutor.isTerminated());
            System.out.println("Hikari pool total = " + hikariDataSource.getHikariPoolMXBean().getTotalConnections());
        }, 0L, 30L, TimeUnit.SECONDS);*/

        try {
            long startTime=System.currentTimeMillis();
            while(true) {
                TimeUnit.MILLISECONDS.sleep(1);
                taskExecutor.execute(() -> {
                    try (Connection conn = hikariDataSource.getConnection();
                         Statement stmt = conn.createStatement()) {
                        stmt.executeQuery("select sleep(1);");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                long endTime=System.currentTimeMillis(); //获取结束时间
                if (endTime - startTime > (1000 * 60 * 10)) {
                    break;
                }
            }
        } finally {
            taskExecutor.shutdownNow();
            hikariDataSource.close();
        }

    }
}
