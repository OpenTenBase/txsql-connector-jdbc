package tdsql.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class DatasourceUuidTest {

    @Test
    public void testCase01() {
        // 全一致
        String url1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
        String url2 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
        Properties prop1 = new Properties();
        prop1.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop1.setProperty(PropertyKey.USER.getKeyName(), "pass");
        Properties prop2 = new Properties();
        prop2.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop2.setProperty(PropertyKey.USER.getKeyName(), "pass");
        String uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url1, prop1));
        String uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url2, prop2));
        assertEquals(uuid1, uuid2);

        // Proxy地址不一致
        url1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15011,9.30.2.116:15011,9.30.2.89:15011,9.30.2.94:15011/test";
        url2 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
        prop1 = new Properties();
        prop1.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop1.setProperty(PropertyKey.USER.getKeyName(), "pass");
        prop2 = new Properties();
        prop2.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop2.setProperty(PropertyKey.USER.getKeyName(), "pass");
        uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url1, prop1));
        uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url2, prop2));
        assertNotEquals(uuid1, uuid2);

        // 数据库名称不一致
        url1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15011,9.30.2.116:15011,9.30.2.89:15011,9.30.2.94:15011/test1";
        url2 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test2";
        prop1 = new Properties();
        prop1.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop1.setProperty(PropertyKey.USER.getKeyName(), "pass");
        prop2 = new Properties();
        prop2.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop2.setProperty(PropertyKey.USER.getKeyName(), "pass");
        uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url1, prop1));
        uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url2, prop2));
        assertNotEquals(uuid1, uuid2);

        // 用户名、密码不一致
        url1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
        url2 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test";
        prop1 = new Properties();
        prop1.setProperty(PropertyKey.USER.getKeyName(), "user_rw");
        prop1.setProperty(PropertyKey.USER.getKeyName(), "pass_rw");
        prop2 = new Properties();
        prop2.setProperty(PropertyKey.USER.getKeyName(), "user_ro");
        prop2.setProperty(PropertyKey.USER.getKeyName(), "pass_ro");
        uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url1, prop1));
        uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url2, prop2));
        assertNotEquals(uuid1, uuid2);

        // URL属性不一致
        url1 = "jdbc:tdsql-mysql:direct://9.30.0.250:15011,9.30.2.116:15011,9.30.2.89:15011,9.30.2.94:15011/test?tdsqlDirectReadWriteMode=rw";
        url2 = "jdbc:tdsql-mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/test?tdsqlDirectReadWriteMode=ro";
        prop1 = new Properties();
        prop1.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop1.setProperty(PropertyKey.USER.getKeyName(), "pass");
        prop2 = new Properties();
        prop2.setProperty(PropertyKey.USER.getKeyName(), "user");
        prop2.setProperty(PropertyKey.USER.getKeyName(), "pass");
        uuid1 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url1, prop1));
        uuid2 = TdsqlDataSourceUuidGenerator.generateUuid(ConnectionUrl.getConnectionUrlInstance(url2, prop2));
        assertNotEquals(uuid1, uuid2);
    }
}
