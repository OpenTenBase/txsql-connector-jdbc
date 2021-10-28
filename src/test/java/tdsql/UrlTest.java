package tdsql;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UrlTest {

    private static final String DB_CLASS = "com.tencent.tdsql.mysql.cj.jdbc.Driver";

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DB_CLASS);
    }

    @Test
    public void case01() {
        connect("jdbc:tdsql-mysql://9.134.209.89:3357/jdbc_test_db");
    }

    @Test
    public void case02() {
        connect("jdbc:tdsql-mysql://9.134.209.89:3357,9.134.209.89:3358,9.134.209.89:3359,9.134.209.89:3360/jdbc_test_db");
    }

    @Test
    public void case03() {
        connect("jdbc:tdsql-mysql:loadbalance://9.134.209.89:3357,9.134.209.89:3358,9.134.209.89:3359,9.134.209.89:3360/jdbc_test_db");
    }

    @Test
    public void case04() {
        connect("jdbc:tdsql-mysql:replication://9.134.209.89:3357,9.134.209.89:3358,9.134.209.89:3359,9.134.209.89:3360/jdbc_test_db");
    }

    @Test
    public void case05() {
        connect("jdbc:tdsql-mysql:loadbalance://9.134.209.89:3357,9.134.209.89:3358,9.134.209.89:3359,9.134.209.89:3360/jdbc_test_db"
                + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=1200"
                + "&haLoadBalanceStrategy=sed"
                + "&haLoadBalanceWeightFactor=1,1,1,1"
                + "&haLoadBalanceBlacklistTimeout=5000"
                + "&haLoadBalanceHeartbeatMonitor=true"
                + "&haLoadBalanceHeartbeatIntervalTime=3000"
                + "&haLoadBalanceMaximumErrorRetries=1");
    }

    private void connect(String url) {
        try (Connection conn = DriverManager.getConnection(url, "root", "123456")) {
            assertNotNull(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
