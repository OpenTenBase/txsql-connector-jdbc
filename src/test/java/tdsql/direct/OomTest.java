package tdsql.direct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class OomTest {

    private static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
    private static final String USERNAME_1 = "qt4s";
    private static final String PASSWORD_1 = "g<m:7KNDF.L1<^1C";
    private static final String PROP = "?tdsqlDirectTopoRefreshIntervalMillis=1000&tdsqlDirectReconnectProxyIntervalTimeSeconds=60";

    @BeforeAll
    public static void setUp() throws ClassNotFoundException {
        Class.forName(DRIVER_CLASS_NAME);
    }

    /**
     * -Xms16m -Xmx16m -XX:+HeapDumpOnOutOfMemoryError
     * @throws InterruptedException
     * @throws SQLException
     */
    @Test
    public void case01() throws InterruptedException, SQLException {
        for (int i = 1; i <= 10; i++) {
            Connection conn = DriverManager.getConnection(URL + i + PROP, USERNAME_1, PASSWORD_1);
            conn.close();
        }
        TimeUnit.HOURS.sleep(6);
    }

    @Test
    public void case02() throws SQLException, InterruptedException {
        int i = 100000;
        Connection conn = DriverManager.getConnection("jdbc:tdsql-mysql:loadbalance://"
                + "9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/qt4s"
                + "?retriesAllDown=4"
                + "&loadBalanceBlocklistTimeout=30000"
                + "&loadBalanceAutoCommitStatementThreshold=1"
                + "&loadBalancePingTimeout=1000"
                + "&loadBalanceValidateConnectionOnSwapServer=true", USERNAME_1, PASSWORD_1);
        while (i > 0) {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(2);
            stmt.executeQuery("select 1");
            stmt.close();
            i--;
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.HOURS.sleep(1);
    }
}
