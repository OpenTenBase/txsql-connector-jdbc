package tdsql.direct.v1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tdsql.direct.v1.base.BaseTest;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class ProxyTest extends BaseTest {

    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.0.250:15012/test"
            + "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&tdsqlReadWriteMode=rw&tdsqlMaxSlaveDelay=100&useSSL=false";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";

    @Test
    @Disabled
    public void testOneProxyDown() throws SQLException, InterruptedException {
        for (; ; ) {
            Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            rs.next();
            System.out.println(rs.getString(1));
            rs.close();
            stmt.close();
            conn.close();

            TimeUnit.SECONDS.sleep(1);
        }
    }
}
