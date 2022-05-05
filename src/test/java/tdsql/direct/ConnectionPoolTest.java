package tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tdsql.base.TdsqlBaseTest;

public class ConnectionPoolTest extends TdsqlBaseTest {

    private static HikariDataSource ds;


    @Test
    public void case01() throws SQLException {
        try (Connection conn = ds.getConnection()) {
            System.out.println("conn = " + conn);
        }
    }

    @Test
    public void case02() throws InterruptedException {
        int cnt = 20;
        CountDownLatch latch = new CountDownLatch(cnt);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < cnt; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try (Connection conn = ds.getConnection()) {
                        TimeUnit.SECONDS.sleep(1);
                        for (Entry<String, List<JdbcConnection>> entry : TdsqlDirectConnectionManager.getInstance()
                                .getAllConnection().entrySet()) {
                            System.out.println(entry.getKey() + ": " + entry.getValue().size());
                        }
                    } catch (SQLException | InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
    }

    @Test
    public void case03() throws InterruptedException {
        case02();
        ds.close();
        init();
        case02();
    }

    @BeforeAll
    public static void init() {
        int min = 20;
        ds = new HikariDataSource();
        ds.setUsername("root");
        ds.setPassword("123456");
        ds.setMinimumIdle(min);
        ds.setMaximumPoolSize(min);
        ds.setJdbcUrl("jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db"
                + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=10000");
    }

    @AfterAll
    public static void cleanup() {
        ds.close();
    }
}
