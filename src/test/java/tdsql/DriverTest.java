package tdsql;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrlParser;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map.Entry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class DriverTest {

    @Test
    public void case01() {
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("com.mysql.cj.jdbc.Driver");
        });
    }

    @Test
    public void case02() {
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        });
    }

    @Test
    public void case03() {
        String url_1 = "jdbc:mysql://1.1.1.1:3306/db?k1=v1&k2=v2";
        String url_2 = "jdbc:tdsql-mysql://1.1.1.1:3306/db?k1=v1&k2=v2";
        ConnectionUrlParser parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        ConnectionUrlParser parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql:loadbalance://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql:loadbalance://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql:loadbalance://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql:loadbalance://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql:direct://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql:direct://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql:direct://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql:direct://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql:replication://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql:replication://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql:replication://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql:replication://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "mysqlx://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "tdsql-mysqlx://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "mysqlx://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "tdsql-mysqlx://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);
    }

    @Test
    public void case04() {
        String url_1 = "jdbc:mysql+srv://1.1.1.1:3306/db?k1=v1&k2=v2";
        String url_2 = "jdbc:tdsql-mysql+srv://1.1.1.1:3306/db?k1=v1&k2=v2";
        ConnectionUrlParser parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        ConnectionUrlParser parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql+srv://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql+srv://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql+srv:loadbalance://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql+srv:loadbalance://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:tdsql-mysql+srv:loadbalance://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:mysql+srv:loadbalance://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql+srv:replication://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql+srv:replication://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "jdbc:mysql+srv:replication://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "jdbc:tdsql-mysql+srv:replication://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "mysqlx+srv://1.1.1.1:3306/db?k1=v1&k2=v2";
        url_2 = "tdsql-mysqlx+srv://1.1.1.1:3306/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);

        url_1 = "mysqlx+srv://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        url_2 = "tdsql-mysqlx+srv://1.1.1.1:3306,2.2.2.2:3307/db?k1=v1&k2=v2";
        parser_1 = ConnectionUrlParser.parseConnectionString(url_1);
        parser_2 = ConnectionUrlParser.parseConnectionString(url_2);
        parserEquals(parser_1, parser_2);
    }

    @Test
    public void case05() {
        String username = "qt4s";
        String password = "g<m:7KNDF.L1<^1C";

        String[] url = new String[]{
                "jdbc:tdsql-mysql://9.30.2.116:15012/test",
                "jdbc:mysql://9.30.2.116:15012/test"
        };
        countEquals(url, username, password);

        url = new String[]{
                "jdbc:tdsql-mysql:loadbalance://9.30.2.116:15012/test",
                "jdbc:mysql:loadbalance://9.30.2.116:15012/test"
        };
        countEquals(url, username, password);

        url = new String[]{
                "jdbc:tdsql-mysql:direct://9.30.2.116:15012/test",
                "jdbc:mysql:direct://9.30.2.116:15012/test"
        };
        countEquals(url, username, password);

        url = new String[]{
                "jdbc:tdsql-mysql:replication://9.30.2.116:15012/test",
                "jdbc:mysql:replication://9.30.2.116:15012/test"
        };
        countEquals(url, username, password);
    }

    private void parserEquals(ConnectionUrlParser parser_1, ConnectionUrlParser parser_2) {
        Assertions.assertEquals(parser_1.getHosts().size(), parser_2.getHosts().size());
        int size = parser_1.getHosts().size();
        for (int i = 0; i < size; i++) {
            HostInfo hi_1 = parser_1.getHosts().get(i);
            HostInfo hi_2 = parser_2.getHosts().get(i);
            Assertions.assertTrue(hi_1.equalHostPortPair(hi_2));
        }
        Assertions.assertEquals(parser_1.getAuthority(), parser_2.getAuthority());
        Assertions.assertEquals(parser_1.getPath(), parser_2.getPath());
        Assertions.assertEquals(parser_1.getQuery(), parser_2.getQuery());
        Assertions.assertEquals(parser_1.getProperties().size(), parser_2.getProperties().size());
        Assertions.assertTrue(parser_1.getProperties().keySet().containsAll(parser_2.getProperties().keySet()));
        Assertions.assertTrue(parser_2.getProperties().keySet().containsAll(parser_1.getProperties().keySet()));
        for (Entry<String, String> entry : parser_1.getProperties().entrySet()) {
            Assertions.assertEquals(entry.getValue(), parser_2.getProperties().get(entry.getKey()));
        }
    }

    private void countEquals(String[] url, String username, String password) {
        final int[] cnt = new int[2];
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(url[0], username, password);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select count(*) from t1")) {
                while (rs.next()) {
                    cnt[0] = rs.getInt(1);
                }
            }
        });
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(url[1], username, password);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select count(*) from t1")) {
                while (rs.next()) {
                    cnt[1] = rs.getInt(1);
                }
            }
        });
        Assertions.assertEquals(cnt[0], cnt[1]);
    }
}
