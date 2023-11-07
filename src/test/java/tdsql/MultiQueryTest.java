package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MultiQueryTest {

    public static Connection getConn(String props) {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");

            String proxyUrl = "jdbc:tdsql-mysql://9.30.2.116:15018/test";
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

    public static void main(String[] args) throws SQLException {
        StatementTest.case01();
        StatementTest.case02();
        StatementTest.case03();
        StatementTest.case04();
        PrepareStatementTest.case01();
        PrepareStatementTest.case02();
        PrepareStatementTest.case03();
        PrepareStatementTest.case04();
    }

    public static class StatementTest {

        /**
         * 不发送 con_set_option
         */
        private static void case01() throws SQLException {
            Connection conn = getConn("allowMultiQueries=true");
            Statement stmt = conn.createStatement();
            stmt.addBatch("update test_1 set username = '111' where id = 1");
            stmt.executeBatch();
            stmt.close();
            conn.close();
        }

        /**
         * 发送 con_set_option
         */
        private static void case02() throws SQLException {
            Connection conn = getConn("allowMultiQueries=false&rewriteBatchedStatements=true");
            Statement stmt = conn.createStatement();
            // SQL个数 > 4个
            stmt.addBatch("update test_1 set username = '111' where id = 1");
            stmt.addBatch("update test_1 set username = '222' where id = 1");
            stmt.addBatch("update test_1 set username = '333' where id = 1");
            stmt.addBatch("update test_1 set username = '444' where id = 1");
            stmt.addBatch("update test_1 set username = '555' where id = 1");
            stmt.executeBatch();
            stmt.close();
            conn.close();
        }

        /**
         * 不发送 con_set_option
         */
        private static void case03() throws SQLException {
            Connection conn = getConn("allowMultiQueries=false&rewriteBatchedStatements=true");
            Statement stmt = conn.createStatement();
            // SQL个数 <= 4个
            stmt.addBatch("update test_1 set username = '111' where id = 1");
            stmt.addBatch("update test_1 set username = '222' where id = 1");
            stmt.addBatch("update test_1 set username = '333' where id = 1");
            stmt.addBatch("update test_1 set username = '444' where id = 1");
            stmt.executeBatch();
            stmt.close();
            conn.close();
        }

        /**
         * 不发送 con_set_option
         */
        private static void case04() throws SQLException {
            Connection conn = getConn("allowMultiQueries=false&rewriteBatchedStatements=false");
            Statement stmt = conn.createStatement();
            // 跟SQL个数无关
            stmt.addBatch("update test_1 set username = '111' where id = 1");
            stmt.executeBatch();
            stmt.close();
            conn.close();
        }
    }

    public static class PrepareStatementTest {

        /**
         * 不发送 con_set_option
         */
        private static void case01() throws SQLException {
            Connection conn = getConn("");
            PreparedStatement psmt = conn.prepareStatement("update test_1 set username = '111' where id = 1");
            psmt.addBatch("update test_1 set username = '222' where id = 1");
            psmt.executeBatch();
            psmt.close();
            conn.close();
        }

        /**
         * 不发送 con_set_option
         */
        private static void case02() throws SQLException {
            Connection conn = getConn("rewriteBatchedStatements=true");
            PreparedStatement psmt = conn.prepareStatement("update test_1 set username = ? where id = 1;");

            // 绑定参数 <= 3 个
            psmt.setString(1, "111");
            psmt.addBatch();

            psmt.executeBatch();
            psmt.close();
            conn.close();
        }

        /**
         * 发送 con_set_option
         */
        private static void case03() throws SQLException {
            Connection conn = getConn("rewriteBatchedStatements=true&allowMultiQueries=false");
            PreparedStatement psmt = conn.prepareStatement("update test_1 set username = ? where id = 1;");

            // 绑定参数 > 3 个
            psmt.setString(1, "111");
            psmt.addBatch();

            psmt.setString(1, "222");
            psmt.addBatch();

            psmt.setString(1, "333");
            psmt.addBatch();

            psmt.setString(1, "444");
            psmt.addBatch();

            psmt.executeBatch();
            psmt.close();
            conn.close();
        }

        /**
         * 不发送 con_set_option
         */
        private static void case04() throws SQLException {
            // allowMultiQueries=true
            Connection conn = getConn("rewriteBatchedStatements=true&allowMultiQueries=TRUE");
            PreparedStatement psmt = conn.prepareStatement("update test_1 set username = ? where id = 1;");

            psmt.setString(1, "111");
            psmt.addBatch();

            psmt.setString(1, "222");
            psmt.addBatch();

            psmt.executeBatch();
            psmt.close();
            conn.close();
        }
    }
}
