package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CountTest {

    private static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:tdsql-mysql:loadbalance://" +
            "tdsqlshard-j9cybzl8.sql.tencentcdb.com:16," +
            "tdsqlshard-axvsyeas.sql.tencentcdb.com:44," +
            "tdsqlshard-e07e0ois.sql.tencentcdb.com:46," +
            "tdsqlshard-p9or8etq.sql.tencentcdb.com:48" +
            "/mysql"
            + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=1200"
            + "&haLoadBalanceStrategy=sed"
            + "&haLoadBalanceWeightFactor=1,1,1,1"
            + "&haLoadBalanceBlacklistTimeout=5000"
            + "&haLoadBalanceHeartbeatMonitor=true"
            + "&haLoadBalanceHeartbeatIntervalTime=3000"
            + "&haLoadBalanceMaximumErrorRetries=1";
    private static final String USERNAME = "tdsqluser";
    private static final String PASSWORD = "Tdsql@2022";

    @Test
    @Disabled("无限循环的测试无法建立流水线")
    public void case01() throws Exception {
        Class.forName(DRIVER_CLASS_NAME);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2000, 2000, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        while (true) {
            threadPoolExecutor.execute(new JdbcTask(URL, USERNAME, PASSWORD));
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    public static class JdbcTask implements Runnable {

        private final String url;
        private final String username;
        private final String password;

        public JdbcTask(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }

        @Override
        public void run() {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
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
}
