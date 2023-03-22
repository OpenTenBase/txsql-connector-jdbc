package tdsql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClientInfoEnableTest {

    private String proxy1 = "9.30.0.250:15012";

    private String proxy2 = "9.30.2.116:15012";

    private String proxy3 = "9.30.2.89:15012";

    private String proxy4 = "9.30.2.94:15012";
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
    public void testWithoutTdsqlSendClientInfoEnable() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy1 + "/test?connectionAttributes=tdsqlA:A,tdsql_B:B";
        execute(connUrl);
    }

    @Test
    public void testWithFalseOpt() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy1 + "/test?tdsqlSendClientInfoEnable=false&connectionAttributes=tdsqlA:A,tdsql_B:B";
        execute(connUrl);
    }

    @Test
    public void testSingleConnectionWithoutOtherParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy1 + "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B";
        execute(connUrl);
    }

    @Test
    public void testSingleConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql://" + proxy1 + "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B" +
                "&passwordCharacterEncoding=utf-8" +
                "&characterEncoding=utf-8" +
                "&connectionTimeZone=Asia/Shanghai" +
                "&socketTimeout=10000";
        execute(connUrl);
    }

    @Test
    public void testTdSqlLBConnectionWithoutOtherParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql:loadbalance://" + proxy1 + "," + proxy2 + "," + proxy3 + "," + proxy4 +
                "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B" +
                "&tdsqlLoadBalanceStrategy=sed" +
                "&tdsqlLoadBalanceWeightFactor=1,1,1,1" +
                "&tdsqlLoadBalanceHeartbeatMonitorEnable=true" +
                "&tdsqlLoadBalanceHeartbeatIntervalTimeMillis=1000" +
                "&tdsqlLoadBalanceHeartbeatMaxErrorRetries=1";
        execute(connUrl);
    }

    @Test
    public void testTdSqlLBConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql:loadbalance://" + proxy1 + "," + proxy2 + "," + proxy3 + "," + proxy4 +
                "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B" +
                "&tdsqlLoadBalanceStrategy=sed" +
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
        String connUrl = "jdbc:tdsql-mysql:direct://" + proxy1 +
                "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B" +
                "&tdsqlDirectReadWriteMode=ro" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true";
        execute(connUrl);
    }

    @Test
    public void testTdSqlDirectConnectionWithAllParams() throws SQLException {
        String connUrl = "jdbc:tdsql-mysql:direct://" + proxy1 +
                "/test?tdsqlSendClientInfoEnable=true&connectionAttributes=tdsqlA:A,tdsql_B:B" +
                "&passwordCharacterEncoding=utf-8" +
                "&characterEncoding=utf-8" +
                "&connectionTimeZone=Asia/Shanghai" +
                "&socketTimeout=10000" +
                "&tdsqlDirectReadWriteMode=ro" +
                "&tdsqlDirectMaxSlaveDelaySeconds=1800" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true" +
                "&tdsqlDirectTopoRefreshIntervalMillis=1000" +
                "&tdsqlLoadBalanceStrategy=sed";
        execute(connUrl);
    }
}
