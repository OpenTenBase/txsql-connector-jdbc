package tdsql.loadbalance;

import org.junit.jupiter.api.Test;
import tdsql.loadbalance.base.BaseTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MultiDataSourcesTest extends BaseTest {

    private String lbProperty1 = "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&logger=Slf4JLogger"
            + "&tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";

    private String lbProperty2 = "?tdsqlLoadBalanceStrategy=sed"
            + "&tdsqlLoadBalanceWeightFactor=1,1"
            + "&tdsqlLoadBalanceHeartbeatMonitorEnable=true"
            + "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000"
            + "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";
    public Connection getConn(String connUrl, String userName, String password) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = connUrl;
            try {
                conn = DriverManager.getConnection(proxyUrl, userName, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    private void execute(String url) throws SQLException {
        Connection conn = getConn(url, "qt4s", "g<m:7KNDF.L1<^1C");

        PreparedStatement psmt = conn.prepareStatement("select ?");
        psmt.setInt(1, 1);
        psmt.executeQuery();

        PreparedStatement psmt1 = conn.prepareStatement("select ?");
        psmt1.setInt(1, 1);
        psmt1.executeQuery();

        psmt1.close();
        psmt.close();

        conn.close();
    }

    @Test
    public void testBuildMultiDatasources() throws SQLException {
        String url1 = "jdbc:tdsql-mysql:loadbalance://" + PROXY_1 + "," + PROXY_2+ "," + PROXY_3 + "/test" + lbProperty1;
        Connection conn1 = getConn(url1, USER, PASS);
        String url2 = "jdbc:tdsql-mysql:loadbalance://" + PROXY_1 + "," + PROXY_2 + "," + PROXY_3 + "/test" + lbProperty2;
        Connection conn2 = getConn(url2, USER, PASS);
        conn1.close();
        conn2.close();
    }

    @Test
    public void testBuildOtherDatasourceAfterOneClosed() throws SQLException {
        String url1 = "jdbc:tdsql-mysql:loadbalance://" + PROXY_1 + "," + PROXY_2+ "," + PROXY_3 + "/test" + lbProperty1;
        execute(url1);
        String url2 = "jdbc:tdsql-mysql:loadbalance://" + PROXY_1 + "," + PROXY_2+ "," + PROXY_3 + "/test" + lbProperty2;
        execute(url2);
    }

    @Test
    public void testTwoConnectionWithDifferentAccounts() throws SQLException {
        String url1 = "jdbc:tdsql-mysql:loadbalance://" + PROXY_1 + "," + PROXY_2+ "," + PROXY_3 + "/test" + lbProperty1;
        System.out.println("Start build first connection:");
        Connection conn1 = getConn(url1, USER, PASS);
        System.out.println("Start build Second connection:");
        Connection conn2 = getConn(url1, "test", PASS);
        conn1.createStatement().execute("select 1");
        conn2.createStatement().execute("select 1");
        conn1.close();
        conn2.close();
    }
}
