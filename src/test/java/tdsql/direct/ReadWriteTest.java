package tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.direct.base.BaseTest;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */

public class ReadWriteTest extends BaseTest {

    @TestMethodOrder(OrderAnnotation.class)
    @Disabled
    public static class WriteReadUserInReadWriteModeTest extends BaseTest {

        @Test
        @Order(1)
        public void testDropDatabase() {
            assertDoesNotThrow(() -> INSTANCE.dropDatabase(RW, USER_RW));
        }

        @Test
        @Order(2)
        public void testCreateDatabase() {
            assertDoesNotThrow(() -> INSTANCE.createDatabase(RW, USER_RW));
        }

        @Test
        @Order(3)
        public void testDropTable() {
            assertDoesNotThrow(() -> INSTANCE.dropTable(RW, USER_RW));
        }

        @Test
        @Order(4)
        public void testCreateTable() {
            assertDoesNotThrow(() -> INSTANCE.createTable(RW, USER_RW));
        }

        @Test
        @Order(5)
        public void testInsert() {
            assertDoesNotThrow(() -> INSTANCE.insert(RW, USER_RW));
        }

        @Test
        @Order(6)
        public void testUpdate() {
            assertDoesNotThrow(() -> INSTANCE.update(RW, USER_RW));
        }

        @Test
        @Order(7)
        public void testDelete() {
            assertDoesNotThrow(() -> INSTANCE.delete(RW, USER_RW));
        }

        @Test
        @Order(8)
        public void testSelect() {
            assertDoesNotThrow(() -> INSTANCE.select(RW, USER_RW));
        }
    }

    @TestMethodOrder(OrderAnnotation.class)
    @Disabled
    public static class WriteReadUserInReadOnlyModeTest extends BaseTest {

        @Test
        @Order(1)
        public void testDropDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.dropDatabase(RO, USER_RW),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(2)
        public void testCreateDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.createDatabase(RO, USER_RW),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(3)
        public void testDropTable() {
            assertThrows(CJException.class, () -> INSTANCE.dropTable(RO, USER_RW),
                    "DROP command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(4)
        public void testCreateTable() {
            assertThrows(CJException.class, () -> INSTANCE.createTable(RO, USER_RW),
                    "CREATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(5)
        public void testInsert() {
            assertThrows(CJException.class, () -> INSTANCE.insert(RO, USER_RW),
                    "INSERT command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(6)
        public void testUpdate() {
            assertThrows(CJException.class, () -> INSTANCE.update(RO, USER_RW),
                    "UPDATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(7)
        public void testDelete() {
            assertThrows(CJException.class, () -> INSTANCE.delete(RO, USER_RW),
                    "DELETE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(8)
        public void testSelect() {
            assertDoesNotThrow(() -> INSTANCE.select(RO, USER_RW));
        }
    }

    @TestMethodOrder(OrderAnnotation.class)
    public static class ReadOnlyUserInReadOnlyModeTest extends BaseTest {

        @Test
        @Order(1)
        public void testDropDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.dropDatabase(RO, USER_RO),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(2)
        public void testCreateDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.createDatabase(RO, USER_RO),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(3)
        public void testDropTable() {
            assertThrows(CJException.class, () -> INSTANCE.dropTable(RO, USER_RO),
                    "DROP command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(4)
        public void testCreateTable() {
            assertThrows(CJException.class, () -> INSTANCE.createTable(RO, USER_RO),
                    "CREATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(5)
        public void testInsert() {
            assertThrows(CJException.class, () -> INSTANCE.insert(RO, USER_RO),
                    "INSERT command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(6)
        public void testUpdate() {
            assertThrows(CJException.class, () -> INSTANCE.update(RO, USER_RO),
                    "UPDATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(7)
        public void testDelete() {
            assertThrows(CJException.class, () -> INSTANCE.delete(RO, USER_RO),
                    "DELETE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(8)
        public void testSelect() {
            assertDoesNotThrow(() -> INSTANCE.select(RO, USER_RO));
        }
    }

    @TestMethodOrder(OrderAnnotation.class)
    @Disabled
    public static class ReadOnlyUserInReadWriteModeTest extends BaseTest {

        @Test
        @Order(1)
        public void testDropDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.dropDatabase(RW, USER_RO),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(2)
        public void testCreateDatabase() {
            assertThrows(CJException.class, () -> INSTANCE.createDatabase(RW, USER_RO),
                    "Access denied for user 'test_ro'@'%' to database 'jdbc_direct_db'");
        }

        @Test
        @Order(3)
        public void testDropTable() {
            assertThrows(CJException.class, () -> INSTANCE.dropTable(RW, USER_RO),
                    "DROP command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(4)
        public void testCreateTable() {
            assertThrows(CJException.class, () -> INSTANCE.createTable(RW, USER_RO),
                    "CREATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(5)
        public void testInsert() {
            assertThrows(CJException.class, () -> INSTANCE.insert(RW, USER_RO),
                    "INSERT command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(6)
        public void testUpdate() {
            assertThrows(CJException.class, () -> INSTANCE.update(RW, USER_RO),
                    "UPDATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(7)
        public void testDelete() {
            assertThrows(CJException.class, () -> INSTANCE.delete(RW, USER_RO),
                    "DELETE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
        }

        @Test
        @Order(8)
        public void testSelect() {
            assertDoesNotThrow(() -> INSTANCE.select(RO, USER_RO));
        }
    }

    private void dropDatabase(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(DROP_DATABASE_IF_EXISTS);
        }
    }

    private void createDatabase(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_DATABASE_IF_NOT_EXISTS);
        }
    }

    private void dropTable(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(DROP_TABLE_IF_EXISTS);
        }
    }

    private void createTable(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_IF_NOT_EXISTS);
        }
    }

    private void insert(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                PreparedStatement psmt = conn.prepareStatement(INSERT)) {
            psmt.setString(1, "a");
            psmt.setString(2, "b");
            psmt.setString(3, "c");
            psmt.setString(4, "d");
            psmt.setString(5, "e");
            psmt.setString(6, "f");
            psmt.setString(7, "g");
            psmt.setString(8, "h");
            psmt.setString(9, "i");
            psmt.setString(10, "j");
            psmt.executeUpdate();
        }
    }

    private void update(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                PreparedStatement psmt = conn.prepareStatement(UPDATE)) {
            psmt.setString(1, "z");
            psmt.setInt(2, 6);
            psmt.executeUpdate();
        }
    }

    private void delete(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                PreparedStatement psmt = conn.prepareStatement(DELETE)) {
            psmt.setInt(1, 6);
            psmt.executeUpdate();
        }
    }

    private void select(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        try (Connection conn = getConnection(mode, user);
                PreparedStatement psmt = conn.prepareStatement(SELECT);
                ResultSet rs = psmt.executeQuery();) {
            int cnt = 6;
            while (rs.next()) {
                assertEquals(cnt, rs.getInt(1));
                assertEquals(String.valueOf((char) (cnt - 1 + 'a')), rs.getString(2));
                ++cnt;
            }
        }
    }

    private static final ReadWriteTest INSTANCE = new ReadWriteTest();

    public static final String DROP_DATABASE_IF_EXISTS = "DROP DATABASE IF EXISTS `jdbc_direct_db`";
    public static final String CREATE_DATABASE_IF_NOT_EXISTS = "CREATE DATABASE"
            + " `jdbc_direct_db` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;";
    public static final String DROP_TABLE_IF_EXISTS = "DROP TABLE IF EXISTS `jdbc_direct_tb`;";
    public static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE `jdbc_direct_db`.`jdbc_direct_tb`"
            + "(`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,`name` varchar(255) "
            + "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,"
            + "PRIMARY KEY (`id`)) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;";
    public static final String INSERT = "INSERT INTO `jdbc_direct_db`.`jdbc_direct_tb` (`id`, `name`) VALUES "
            + "(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?),(NULL,?);";
    public static final String UPDATE = "UPDATE `jdbc_direct_db`.`jdbc_direct_tb` SET `name`= ? WHERE id < ?;";
    public static final String DELETE = "DELETE FROM `jdbc_direct_db`.`jdbc_direct_tb` WHERE id < ?";
    public static final String SELECT = "SELECT `id`, `name` FROM `jdbc_direct_db`.`jdbc_direct_tb` ORDER BY `id`";
}
