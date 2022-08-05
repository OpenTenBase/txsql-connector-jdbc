package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

public class URLTest {
    protected static final String URLS1 = "jdbc:tdsql-mysql:direct://"
            + "9.30.1.178:4015,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=ro&tdsqlLoadBalanceStrategy=Lc&tdsqlDirectMasterCarryOptOfReadOnlyMode=true";
    protected static final String USER_S = "tdsqlsys_normal";
    protected static final String PASS_S = "gl%LDY^1&OKWkLWQP^7&";
    Properties info = new Properties();
    @Test
    public void TestUrl(){
        info.setProperty("user", USER_S);
        info.setProperty("password", PASS_S);
        ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(URLS1, info);
        Properties props = conStr.getConnectionArgumentsAsProperties();
        String strategy = props.getProperty(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName(), "Sed");
        String tdsqlDirectMasterCarryOptOfReadOnlyModeStr = props.getProperty(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName(), "false");
        System.out.println(strategy);
        System.out.println(tdsqlDirectMasterCarryOptOfReadOnlyModeStr);
        try {
            Tset();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void Tset() throws SQLException {
        throw new SQLException("meieeie");
    }
}
