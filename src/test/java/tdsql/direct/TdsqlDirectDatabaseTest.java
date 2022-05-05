package tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
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
