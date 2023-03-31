package tdsql.loadbalance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.alibaba.druid.pool.DruidDataSource;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.loadbalance.base.BaseTest;

@TestMethodOrder(OrderAnnotation.class)
public class SendSqlTest extends BaseTest {

    private static final int totalCount = 100;
    private ThreadPoolExecutor executor;
    private ScheduledThreadPoolExecutor scheduledExecutor;
    private static final List<String> keyList = new ArrayList<>(totalCount * 5);
    private static final ConcurrentLinkedQueue<String> insertedQueue = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<String> updatedQueue = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<String> deletedQueue = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<String> selectedQueue = new ConcurrentLinkedQueue<>();

    @BeforeEach
    public void beforeEach() {
        for (int i = 0; i < totalCount * 5; i++) {
            keyList.add(String.valueOf(UUID.randomUUID()) + UUID.randomUUID());
        }
        assertEquals(totalCount * 5, keyList.size());

        for (String proxy : PROXY_ARRAY) {
            String jdbcUrl = String.format("jdbc:tdsql-mysql://%s/%s", proxy, DB_MYSQL);
            try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASS);
                    Statement stmt = conn.createStatement();
                    PreparedStatement psmt = conn.prepareStatement(INSERT)) {
                stmt.executeUpdate(CREATE_DATABASE_IF_NOT_EXISTS);
                stmt.executeUpdate(CREATE_TABLE_IF_NOT_EXISTS);
                stmt.executeUpdate(TRUNCATE_TABLE);

                for (int i = 0, j = 0; i < totalCount; i++) {
                    for (int k = 1; k < 10; k += 2) {
                        String key = keyList.get(j);
                        String name = key.substring(16, 48);
                        psmt.setString(k, name);
                        psmt.setString(k + 1, key);
                        j++;
                    }
                    psmt.executeUpdate();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        insertedQueue.addAll(keyList);

        this.executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(totalCount), new ThreadPoolExecutor.AbortPolicy());

        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        this.scheduledExecutor.scheduleAtFixedRate(() -> System.out.println(
                "Time: " + LocalTime.now() + ", Pool Size: " + this.executor.getPoolSize() + ", Queue Size: "
                        + this.executor.getQueue().size()), 0, 1000, TimeUnit.MILLISECONDS);
    }

    @AfterEach
    public void afterEach() {
        if (!this.executor.isShutdown()) {
            this.executor.shutdownNow();
        }
        if (!this.scheduledExecutor.isShutdown()) {
            this.scheduledExecutor.shutdownNow();
        }
        keyList.clear();
        insertedQueue.clear();
        updatedQueue.clear();
        deletedQueue.clear();
        selectedQueue.clear();
    }

    @Test
    @Order(1)
    public void testHikari() throws Exception {
        try (HikariDataSource ds = (HikariDataSource) super.createHikariDataSource()) {
            this.testExecuteSql(ds);
        }
    }

    @Test
    @Order(2)
    public void testDruid() throws Exception {
        try (DruidDataSource ds = (DruidDataSource) super.createDruidDataSource()) {
            this.testExecuteSql(ds);
        }
    }

    @Test
    @Order(3)
    public void testAtomikos() throws Exception {
        AtomikosDataSourceBean ds = (AtomikosDataSourceBean) super.createAtomikosDataSource();
        this.testExecuteSql(ds);
        ds.close();
    }

    private void testExecuteSql(DataSource ds) throws InterruptedException {
        int updateCnt = 0;
        int deleteCnt = 0;
        int selectCnt = 0;
        CountDownLatch latch = new CountDownLatch(totalCount);
        for (int i = 0; i < totalCount; i++) {
            switch (ThreadLocalRandom.current().nextInt(0, 3)) {
                case 0:
                    updateCnt++;
                    this.executor.execute(new UpdateTask(ds, latch));
                    break;
                case 1:
                    deleteCnt++;
                    this.executor.execute(new DeleteTask(ds, latch));
                    break;
                case 2:
                    selectCnt++;
                    this.executor.execute(new SelectTask(ds, latch));
                    break;
                default:
                    break;
            }
        }
        this.executor.shutdown();
        latch.await();

        assertEquals(totalCount * 5 - updateCnt - deleteCnt - selectCnt, insertedQueue.size());
        assertEquals(updateCnt, updatedQueue.size());
        assertEquals(deleteCnt, deletedQueue.size());
        assertEquals(selectCnt, selectedQueue.size());
    }

    private static class UpdateTask implements Runnable {

        private final DataSource ds;
        private final CountDownLatch latch;

        public UpdateTask(DataSource ds, CountDownLatch latch) {
            this.ds = ds;
            this.latch = latch;
        }

        @Override
        public void run() {
            try (Connection conn = ds.getConnection();
                    PreparedStatement psmt = conn.prepareStatement(UPDATE)) {

                String insertedKey = insertedQueue.poll();
                if (insertedKey != null) {
                    updatedQueue.add(insertedKey);
                    psmt.setString(1, "UPDATED");
                    psmt.setString(2, insertedKey);
                    int effectRows = psmt.executeUpdate();
                    assertEquals(1, effectRows);
                } else {
                    System.out.println("UpdateTask get insertedKey failed.");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class DeleteTask implements Runnable {

        private final DataSource ds;
        private final CountDownLatch latch;

        public DeleteTask(DataSource ds, CountDownLatch latch) {
            this.ds = ds;
            this.latch = latch;
        }

        @Override
        public void run() {
            try (Connection conn = ds.getConnection();
                    PreparedStatement psmt = conn.prepareStatement(DELETE)) {

                String insertedKey = insertedQueue.poll();
                if (insertedKey != null) {
                    deletedQueue.add(insertedKey);
                    psmt.setString(1, insertedKey);
                    int effectRows = psmt.executeUpdate();
                    assertEquals(1, effectRows);
                } else {
                    System.out.println("DeleteTask get insertedKey failed.");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class SelectTask implements Runnable {

        private final DataSource ds;
        private final CountDownLatch latch;

        public SelectTask(DataSource ds, CountDownLatch latch) {
            this.ds = ds;
            this.latch = latch;
        }

        @Override
        public void run() {
            ResultSet rs = null;
            try (Connection conn = ds.getConnection();
                    PreparedStatement psmt = conn.prepareStatement(SELECT)) {

                String insertedKey = insertedQueue.poll();
                if (insertedKey != null) {
                    selectedQueue.add(insertedKey);
                    psmt.setString(1, insertedKey);
                    rs = psmt.executeQuery();
                    while (rs.next()) {
                        String name = rs.getString("name");
                        assertNotNull(name);
                    }
                } else {
                    System.out.println("SelectTask get updatedKey failed.");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            }
        }
    }

    private static final String CREATE_DATABASE_IF_NOT_EXISTS = "CREATE DATABASE IF NOT EXISTS"
            + " `jdbc_loadbalance_db` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;";
    private static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS "
            + "`jdbc_loadbalance_db`.`jdbc_loadbalance_tb`"
            + "(`name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,"
            + "`key` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,"
            + "PRIMARY KEY (`key`)"
            + ") ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;";
    private static final String TRUNCATE_TABLE = "TRUNCATE TABLE `jdbc_loadbalance_db`.`jdbc_loadbalance_tb`;";
    private static final String INSERT = "INSERT INTO `jdbc_loadbalance_db`.`jdbc_loadbalance_tb`"
            + "(`name`, `key`) values (?, ?), (?, ?), (?, ?), (?, ?), (?, ?);";
    private static final String UPDATE = "UPDATE `jdbc_loadbalance_db`.`jdbc_loadbalance_tb` SET `name` = ? WHERE `key` = ?;";
    private static final String DELETE = "DELETE FROM `jdbc_loadbalance_db`.`jdbc_loadbalance_tb` WHERE `key` = ?;";
    private static final String SELECT = "SELECT `name` FROM `jdbc_loadbalance_db`.`jdbc_loadbalance_tb` WHERE `key` = ?;";
}
