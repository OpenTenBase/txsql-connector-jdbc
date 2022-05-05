package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 * @since DBhouse v2.0
 */
public class LocalDateTimeTest {
    public static Connection getConn() {
        return getConn("");
    }

    public static Connection getConn(String props) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = "jdbc:tdsql-mysql://9.135.1.199:15013/dorianzhang";
            if (props != null && !"".equals(props.trim())) {
                proxyUrl += "?" + props;
            }
            try {
                conn = DriverManager.getConnection(proxyUrl, "test", "test");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void main(String[] args) throws SQLException {
        case01();
    }

    private static void case01() throws SQLException {
        Connection conn = getConn();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT DATE_FORMAT(tt, '%Y-%m-%d %H:%i:%s') as tt FROM test_ldt");
        rs.next();
        System.out.println("rs.getObject(1) = " + rs.getObject(1));
        Timestamp t = Timestamp.valueOf(String.valueOf(rs.getObject(1)));
        System.out.println("t = " + t);
        rs.close();
        stmt.close();
        conn.close();
    }
}
