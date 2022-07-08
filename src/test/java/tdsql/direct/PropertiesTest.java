package tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.url.DirectConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectTopoServer;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tdsql.direct.base.BaseTest;

/**
 * <p>数据库直连测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class PropertiesTest extends BaseTest {

    @Test
    public void testTdsqlHostInfo() {
        String url1 = "jdbc:tdsql-mysql:direct://9.30.1.140:15038/test";
        TdsqlHostInfo thi1 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, null).getMainHost());
        TdsqlHostInfo thi2 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, null).getMainHost());
        assertEquals(thi1, thi2);

        String url2 = "jdbc:tdsql-mysql:direct://9.30.1.207:15038/test";
        TdsqlHostInfo thi3 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url2, null).getMainHost());
        assertNotEquals(thi1, thi3);

        String url3 = "jdbc:tdsql-mysql:direct://9.30.1.140:15038/test?param=value&flag=true";
        TdsqlHostInfo thi4 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        TdsqlHostInfo thi5 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        assertEquals(thi4, thi5);

        Properties prop = new Properties();
        prop.put("param", "value");
        prop.put("flag", "true");
        TdsqlHostInfo thi6 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi7 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        assertEquals(thi6, thi7);

        TdsqlHostInfo thi8 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi9 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url2, prop).getMainHost());
        assertNotEquals(thi8, thi9);

        TdsqlHostInfo thi10 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi11 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        assertEquals(thi10, thi11);
    }

    @Test
    public void testTopoRefreshIntervalChanged() throws InterruptedException {
        for (int i = 2; i <= 6; i++) {
            Properties props = new Properties();
            props.put(PropertyKey.tdsqlDirectTopoRefreshIntervalMillis.getKeyName(), String.valueOf(i * 1000L));
            assertDoesNotThrow(() -> {
                try (Connection conn = getConnection(RO, props)) {
                    assertNotNull(conn);
                }
            });
        }
        assertEquals(6 * 1000, TdsqlDirectTopoServer.getInstance().getTdsqlDirectTopoRefreshIntervalMillis());
        TimeUnit.SECONDS.sleep(7);
        assertEquals(6 * 1000, TdsqlDirectTopoServer.getInstance().getTdsqlDirectTopoRefreshIntervalMillis());
    }

    @Test
    public void testDirectConnectionUrl() {
        Properties props = new Properties();
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertEquals(DirectConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:tdsql-mysql:direct://hostname?dnsSrv=false", null).getClass());
        assertEquals(DirectConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:tdsql-mysql:direct://hostname", props).getClass());
        assertEquals(DirectConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:tdsql-mysql:direct://hostname?dnsArv=false", props).getClass());
        assertEquals(DirectConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:tdsql-mysql:direct://hostname?dnsSrv=true", props).getClass());
    }
}
