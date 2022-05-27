package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 * @since DBhouse v2.0
 */
public class TransactionTest {

    public static Connection getConn(String props) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = "jdbc:tdsql-mysql://9.135.1.199:15013/dorianzhang?cachePrepStmts=true&useServerPrepStmts=true";
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
        Connection conn = getConn("");
        PreparedStatement psmt = conn.prepareStatement("select 1");
        ResultSet rs = psmt.executeQuery();
        System.out.println("rs = " + rs);

        rs.close();
        psmt.close();
        conn.close();
    }
}
