package tdsql.base;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectTopoServer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public abstract class TdsqlBaseTest {

    protected static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    protected static final String DB_DIRECT_URL = "jdbc:tdsql-mysql:direct://9.134.209.89:3357/jdbc_test_db";
    protected static final String DB_SIMPLE_URL = "jdbc:tdsql-mysql://9.134.209.89:3357/jdbc_test_db";
    protected Properties props = null;
    protected Connection directConn = null;
    protected Connection simpleConn = null;

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.printf("Running test: %s, method: %s%n",
                testInfo.getTestClass().orElse(Class.forName("java.lang.NullPointerException")).getName(),
                testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        Class.forName(DRIVER_CLASS_NAME);

        this.props = new Properties();
        this.props.setProperty(PropertyKey.USER.getKeyName(), "root");
        this.props.setProperty(PropertyKey.PASSWORD.getKeyName(), "123456");
        this.props.setProperty(PropertyKey.useSSL.getKeyName(), Boolean.FALSE.toString());
        this.props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), TimeZone.getDefault().getID());
        this.props.setProperty(PropertyKey.characterEncoding.getKeyName(), StandardCharsets.UTF_8.name());
//        this.directConn = DriverManager.getConnection(DB_DIRECT_URL, this.props);
//        this.simpleConn = DriverManager.getConnection(DB_SIMPLE_URL, this.props);
    }

    protected Connection getDirectConn() throws SQLException {
        return DriverManager.getConnection(DB_DIRECT_URL, this.props);
    }

    protected Connection getDirectConn(Properties props) throws SQLException {
        props.putAll(this.props);
        return DriverManager.getConnection(DB_DIRECT_URL, props);
    }

    protected void printAllConnection() {
        int total = 0;
        System.out.println("------------------------------ printAllConnection ------------------------------");
        for (Entry<TdsqlHostInfo, List<JdbcConnection>> entry : TdsqlDirectConnectionManager.getInstance()
                .getAllConnection().entrySet()) {
            for (JdbcConnection jdbcConnection : entry.getValue()) {
                ++total;
                System.out.println("Host:Connection = " + entry.getKey() + ": " + jdbcConnection);
            }
        }
        System.out.println("Total: " + total);
        System.out.println("------------------------------ printAllConnection ------------------------------");
    }

    protected void printScheduleQueue() {
        long total = 0;
        System.out.println("------------------------------ printScheduleQueue ------------------------------");
        for (Entry<TdsqlHostInfo, Long> entry : TdsqlDirectTopoServer.getInstance().getScheduleQueue().asMap()
                .entrySet()) {
            total += entry.getValue();
            System.out.println("Host:Count = " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("Total: " + total);
        System.out.println("------------------------------ printScheduleQueue ------------------------------");
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (this.simpleConn != null) {
            this.simpleConn.close();
        }

        if (this.directConn != null) {
            this.directConn.close();
        }
    }
}
