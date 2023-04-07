package tdsql.direct.v1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCacheTopologyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tdsql.direct.v1.base.BaseTest;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class ProxyTest extends BaseTest {

    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/testdb"
            + "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&tdsqlReadWriteMode=ro&tdsqlDirectMaxSlaveDelaySeconds=10&useSSL=false&tdsqlDirectReadWriteMode=ro&connectTimeout=20000";
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

    @Test
    @Disabled
    public void testSlaveDelay() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                for (;;) {
                    try {
                        Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
                        for (; ; ) {
                            Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery("select 1");
                            rs.next();
                            System.out.println(rs.getString(1));
                            rs.close();
                            stmt.close();

                            TimeUnit.SECONDS.sleep(1);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        for (;;) {}
    }

    @Test
    public void testConnectNotExistedProxy() {
        try{
            Connection conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            Statement stmt = conn.createStatement();
             stmt.executeQuery("select 1");
            Assertions.fail("this connection shouldn't be created successfully!");
        } catch (Throwable e) {
            e.printStackTrace();
            if (!TdsqlDirectCacheTopologyException.class.isInstance(e))
                Assertions.fail("unexpected exception typ[e:" + e.getClass().getName());
        }

    }
}
