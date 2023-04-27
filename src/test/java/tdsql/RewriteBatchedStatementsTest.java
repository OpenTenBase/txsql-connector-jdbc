package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class RewriteBatchedStatementsTest {

    public static Connection getConn(String props) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = "jdbc:tdsql-mysql://9.30.2.89:15012/test";
            if (props != null && !"".equals(props.trim())) {
                proxyUrl += "?" + props;
            }
            try {
                conn = DriverManager.getConnection(proxyUrl, "qt4s", "g<m:7KNDF.L1<^1C");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    @BeforeEach
    public void setUp() throws SQLException {
        Connection conn = getConn("");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("truncate table t1");
        stmt.close();
        conn.close();
    }

    @Test
    public void testCase01() throws SQLException {
        Connection conn = getConn("rewriteBatchedStatements=true");
        conn.setAutoCommit(false);
        PreparedStatement psmt = conn.prepareStatement("insert into t1(id, name) values (?, ?)");

        for (int i = 100; i < 102; i++) {
            psmt.setInt(1, i);
            psmt.setString(2, String.valueOf(i));
            psmt.addBatch();
        }

        psmt.executeBatch();
        conn.commit();
        psmt.close();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select name from t1 where id in (100, 101, 102, 103)");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        rs.close();
        stmt.close();
        conn.close();
    }
}
