package tdsql.loadbalance;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MultiDataSourcesTest {

    private String proxy1 = "9.30.0.250:15012";

    private String proxy2 = "9.30.2.116:15012";

    private String proxy3 = "9.30.2.89:15012";

    private String proxy4 = "9.30.2.94:15012";

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
    public Connection getConn(String connUrl) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = connUrl;
            try {
                conn = DriverManager.getConnection(proxyUrl, "qt4s", "g<m:7KNDF.L1<^1C");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    private void execute(String url) throws SQLException {
        Connection conn = getConn(url);

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
    public void testBuildMultiDatasources() {
        String url1 = "jdbc:tdsql-mysql:loadbalance://" + proxy1 + "," + proxy2 + "," + proxy3 + "," + proxy4 + "/test" + lbProperty1;
        getConn(url1);
        String url2 = "jdbc:tdsql-mysql:loadbalance://" + proxy1 + "," + proxy2 + "," + proxy3 + "," + proxy4 + "/test" + lbProperty2;
        getConn(url2);
    }
}
