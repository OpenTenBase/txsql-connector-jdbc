package tdsql.direct.v2.util;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 数据源UUID生成器单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDataSourceUuidGeneratorTest extends TdsqlDirectBaseTest {

    @Test
    public void testCase01() {
        String url1 = "jdbc:tdsql-mysql:direct://1.1.1.1:15001,2.2.2.2:15001,3.3.3.3:15001/test" +
                "?useLocalSessionStates=true" +
                "&useUnicode=true" +
                "&characterEncoding=utf-8" +
                "&serverTimezone=Asia/Shanghai" +
                "&tdsqlDirectReadWriteMode=rw" +
                "&tdsqlDirectTopoRefreshIntervalMillis=500" +
                "&tdsqlDirectMaxSlaveDelaySeconds=0" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=false" +
                "&tdsqlLoadBalanceStrategy=lc" +
                "&logger=Slf4JLogger" +
                "&autoReconnect=true" +
                "&socketTimeout=1000";

        String url2 = "jdbc:tdsql-mysql:direct://1.1.1.1:15001,2.2.2.2:15001,3.3.3.3:15001/test" +
                "?useLocalSessionStates=true" +
                "&useUnicode=true" +
                "&characterEncoding=utf-8" +
                "&serverTimezone=Asia/Shanghai" +
                "&tdsqlDirectReadWriteMode=ro" +
                "&tdsqlDirectTopoRefreshIntervalMillis=500" +
                "&tdsqlDirectMaxSlaveDelaySeconds=300" +
                "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true" +
                "&tdsqlLoadBalanceStrategy=lc" +
                "&logger=Slf4JLogger" +
                "&autoReconnect=true" +
                "&socketTimeout=1000";

        ConnectionUrl conStr1 = ConnectionUrl.getConnectionUrlInstance(url1, null);
        String uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(conStr1);

        ConnectionUrl conStr2 = ConnectionUrl.getConnectionUrlInstance(url2, null);
        String uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(conStr2);

        Assertions.assertNotEquals(uuid1, uuid2);
    }
}
