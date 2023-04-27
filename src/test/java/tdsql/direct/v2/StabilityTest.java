package tdsql.direct.v2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class StabilityTest {

    private static final String URL =
            "jdbc:mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/qt4s?"
                    + "useLocalSessionStates=false" +
                    "&useUnicode=true" +
                    "&tdsqlDirectReadWriteMode=ro" +
                    "&characterEncoding=utf-8" +
                    "&tdsqlDirectMaxSlaveDelaySeconds=20" +
                    "&serverTimezone=Asia/Shanghai" +
                    "&tdsqlDirectTopoRefreshIntervalMillis=1000" +
                    "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
                    "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=500" +
                    "&tdsqlDirectCloseConnTimeoutMillis=500" +
                    "&tdsqlLoadBalanceStrategy=sed" +
                    "&logger=Slf4JLogger";
    private static final int numConn = 100;

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(numConn);
        List<Connection> connList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numConn; i++) {
            executorService.execute(() -> {
                Connection conn;
                try {
                    conn = DriverManager.getConnection(URL, "qt4s", "g<m:7KNDF.L1<^1C");
                    Assertions.assertNotNull(conn);
                    connList.add(conn);
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executorService.shutdown();

        TimeUnit.HOURS.sleep(1);

        for (Connection connection : connList) {
            connection.close();
        }
    }
}
