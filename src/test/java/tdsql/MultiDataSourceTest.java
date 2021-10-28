package tdsql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MultiDataSourceTest {

    private static final String DB_CLASS = "com.tencent.tdsql.mysql.cj.jdbc.Driver";

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DB_CLASS);
    }

    @Test
    public void case01() throws SQLException {
        HikariDataSource ds1 = (HikariDataSource) this.createDataSource("9.134.209.89:3357,9.134.209.89:3358",
                "1,2");
        HikariDataSource ds2 = (HikariDataSource) this.createDataSource("9.134.209.89:3359,9.134.209.89:3360",
                "3,4");

        Connection conn1 = ds1.getConnection();
        Assertions.assertNotNull(conn1);

        Connection conn2 = ds2.getConnection();
        Assertions.assertNotNull(conn2);
    }

    @Test
    public void case02() throws SQLException {
        HikariDataSource ds1 = (HikariDataSource) this.createDataSource("9.134.209.89:3357,9.134.209.89:3358",
                "1,2,3,4");
        HikariDataSource ds2 = (HikariDataSource) this.createDataSource("9.134.209.89:3359,9.134.209.89:3360",
                "3");

        Connection conn1 = ds1.getConnection();
        Assertions.assertNotNull(conn1);

        Connection conn2 = ds2.getConnection();
        Assertions.assertNotNull(conn2);
    }

    @Test
    public void case03() throws SQLException {
        HikariDataSource ds1 = (HikariDataSource) this.createDataSource("9.134.209.89:3357,9.134.209.89:3358",
                "1");
        HikariDataSource ds2 = (HikariDataSource) this.createDataSource("9.134.209.89:3359,9.134.209.89:3360",
                "2,3,4,5");

        Connection conn1 = ds1.getConnection();
        Assertions.assertNotNull(conn1);

        Connection conn2 = ds2.getConnection();
        Assertions.assertNotNull(conn2);
    }

    private DataSource createDataSource(String hostPort, String haLoadBalanceWeightFactor) {
        HikariDataSource ds = new HikariDataSource();
        ds.setUsername("root");
        ds.setPassword("123456");
        ds.setMinimumIdle(1);
        ds.setMaximumPoolSize(1);
        ds.setMaxLifetime(30000);
        ds.setJdbcUrl(
                "jdbc:tdsql-mysql:loadbalance://" + hostPort + "/jdbc_test_db"
                        + "?useSSL=false&useUnicode=true&characterEncoding=UTF-8&socketTimeout=3000&connectTimeout=1200"
                        + "&haLoadBalanceStrategy=sed"
                        + "&haLoadBalanceWeightFactor="
                        + haLoadBalanceWeightFactor
                        + "&haLoadBalanceBlacklistTimeout=5000"
                        + "&haLoadBalanceHeartbeatMonitor=true"
                        + "&haLoadBalanceHeartbeatIntervalTime=3000"
                        + "&haLoadBalanceMaximumErrorRetries=1");
        return ds;
    }
}
