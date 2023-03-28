package tdsql.multids;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class MultiCreateConn {

    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.0.250:15012/test" +
            "?passwordCharacterEncoding=latin1&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useUnicode=true&useSSL=false&connectTimeout=10000&socketTimeout=60000&allowMultiQueries=true&logger=NullLogger"
            + "&tdsqlDirectTopoRefreshIntervalMillis=1000"
            ;
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";
    private static final int tNum = 500;

    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(tNum);
        ExecutorService executorService = Executors.newFixedThreadPool(tNum);
        for (int i = 0; i < tNum; i++) {
            executorService.execute(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("select count(*) from t_user;")
                ) {
                    System.out.println(
                            "===============================" + Thread.currentThread().getId() + " finished!");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });

        }
        latch.await();
        executorService.shutdown();
    }
}
