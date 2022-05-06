package tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdsql.base.TdsqlBaseTest;

/**
 * <p>数据库直连测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectDatabaseTest extends TdsqlBaseTest {

    @Test
    public void testTdsqlHostInfo() {
        String url1 = "jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db";
        TdsqlHostInfo thi1 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, null).getMainHost());
        TdsqlHostInfo thi2 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, null).getMainHost());
        Assertions.assertEquals(thi1, thi2);

        String url2 = "jdbc:tdsql-mysql:direct://9.134.209.89:3358/jdbc_test_db";
        TdsqlHostInfo thi3 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url2, null).getMainHost());
        Assertions.assertNotEquals(thi1, thi3);

        String url3 = "jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db?param=value&flag=true";
        TdsqlHostInfo thi4 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        TdsqlHostInfo thi5 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        Assertions.assertEquals(thi4, thi5);

        Properties prop = new Properties();
        prop.put("param", "value");
        prop.put("flag", "true");
        TdsqlHostInfo thi6 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi7 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        Assertions.assertEquals(thi6, thi7);

        TdsqlHostInfo thi8 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi9 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url2, prop).getMainHost());
        Assertions.assertNotEquals(thi8, thi9);

        TdsqlHostInfo thi10 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url1, prop).getMainHost());
        TdsqlHostInfo thi11 = new TdsqlHostInfo(
                ConnectionUrl.getConnectionUrlInstance(url3, null).getMainHost());
        Assertions.assertEquals(thi10, thi11);
    }

    @Test
    public void testTopoRefreshIntervalChanged() throws InterruptedException {
        int count = 20;
        CountDownLatch cdl = new CountDownLatch(count);
        ExecutorService executorService = Executors.newFixedThreadPool(count * 2);
        for (int i = 2; i <= count + 1; i++) {
            int finalI = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Properties props = new Properties();
                    props.put(PropertyKey.tdsqlProxyTopoRefreshInterval.getKeyName(), String.valueOf(finalI * 100L));
                    try (Connection conn = getDirectConn(props)) {
                        System.out.println("Thread-" + finalI + "'s conn = " + conn);
                        TimeUnit.SECONDS.sleep(1);
                        cdl.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        Assertions.assertTrue(cdl.await(5, TimeUnit.SECONDS));
        System.out.println("TdsqlDirectTopoServer.getInstance().getTdsqlProxyTopoRefreshInterval() = "
                + TdsqlDirectTopoServer.getInstance().getTdsqlProxyTopoRefreshInterval());
    }
}
