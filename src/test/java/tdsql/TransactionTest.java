package tdsql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

            String proxyUrl = "jdbc:tdsql-mysql://9.30.1.207:15006/db1";
            if (props != null && !"".equals(props.trim())) {
                proxyUrl += "?" + props;
            }
            try {
                conn = DriverManager.getConnection(proxyUrl, "joseph", "Aaaaaaaa1!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void main(String[] args) throws SQLException {
        Connection conn = getConn("useServerPrepStmts=true&cachePrepStmts=true");

        PreparedStatement psmt = conn.prepareStatement("select ?");
        psmt.setInt(1, 1);
        psmt.executeQuery();

        PreparedStatement psmt1 = conn.prepareStatement("select ?");
        psmt1.setInt(1, 1);
        psmt1.executeQuery();

        psmt1.close();
        psmt.close();

        conn.close();
    }
}
