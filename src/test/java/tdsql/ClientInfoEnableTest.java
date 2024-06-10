package tdsql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClientInfoEnableTest {

    private String proxy2 = "175.27.165.52:15002";

    private String proxy3 = "119.45.153.189:15002";

    private String proxy4 = "9.30.2.94:15016";
    public Connection getConn(String connUrl) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = connUrl;
            try {
                conn = DriverManager.getConnection(proxyUrl, "tdwtest", "Abcd_1234_.");
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
    public void testWithoutTdsqlSendClientInfoEnable() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy2 + "/test?";
        execute(connUrl);
    }

    @Test
    public void testWithFalseOpt() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy2 + "/test?tdsqlSendClientInfoEnable=false";
        execute(connUrl);
    }

    @Test
    public void testSingleConnectionWithoutOtherParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy2 + "/test?tdsqlSendClientInfoEnable=true";
        execute(connUrl);
    }

    @Test
    public void testSingleConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:mysql://" + proxy2 + "/sbtest?tdsqlSendClientInfoEnable=true" +
                "&passwordCharacterEncoding=utf-8" +
                "&characterEncoding=utf-8" +
                "&connectionTimeZone=Asia/Shanghai" +
                "&socketTimeout=10000";
        execute(connUrl);
    }

    @Test
    public void testTdSqlLBConnectionWithoutOtherParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql:loadbalance://" +  proxy2 + "," + proxy3 +
                "/test?tdsqlSendClientInfoEnable=true" +
                "&tdsqlLoadBalanceStrategy=sed" +
                "&tdsqlLoadBalanceWeightFactor=1,1,1,1" +
                "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000" +
                "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";
        execute(connUrl);
    }

    @Test
    public void testTdSqlLBConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:mysql:loadbalance://" + proxy2 + "," + proxy3 +
                "/test?tdsqlSendClientInfoEnable=true" +
                "&tdsqlLoadBalanceStrategy=lc" +
                "&tdsqlLoadBalanceWeightFactor=1,1,1,1" +
                "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000" +
                "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1" +
                "&passwordCharacterEncoding=utf-8" +
                "&characterEncoding=utf-8" +
                "&connectionTimeZone=Asia/Shanghai" +
                "&socketTimeout=10000";
        execute(connUrl);
    }

    @Test
    public void testTdSqlDirectConnectionWithoutOtherParams() throws SQLException {
        String connUrl = "jdbc:mysql:direct://" + proxy2 +
                "/test?tdsqlSendClientInfoEnable=true" +
                "&tdsqlDirectReadWriteMode=ro" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true";
        execute(connUrl);
    }

    @Test
    public void testTdSqlDirectConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql:direct://" + proxy2 +
                "/test?tdsqlSendClientInfoEnable=true" +
                "&passwordCharacterEncoding=utf-8" +
                "&characterEncoding=utf-8" +
                "&connectionTimeZone=Asia/Shanghai" +
                "&socketTimeout=10000" +
                "&connectTimeout=1000" +
                "&tdsqlDirectReadWriteMode=ro" +
                "&tdsqlDirectMaxSlaveDelaySeconds=1800" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true" +
                "&tdsqlDirectTopoRefreshIntervalMillis=1000" +
                "&tdsqlLoadBalanceStrategy=lc";
        execute(connUrl);
    }
}
