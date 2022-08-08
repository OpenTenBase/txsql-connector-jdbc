package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.base;

import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WriteAndReadTest extends BaseTest{

    @Test
    @Order(1)
    public void testDropDatabase() {
        assertDoesNotThrow(() -> INSTANCE.dropDatabase(RW, URL_RW));
    }

    @Test
    @Order(2)
    public void testCreateDatabase() {
        assertDoesNotThrow(() -> INSTANCE.createDatabase(RW, URL_RW));
    }

    @Test
    @Order(3)
    public void testDropTable() {
        assertDoesNotThrow(() -> INSTANCE.dropTable(RW, URL_RW));
    }

    @Test
    @Order(4)
    public void testCreateTable() {
        assertDoesNotThrow(() -> INSTANCE.createTable(RW, URL_RW));
    }

    @Test
    @Order(5)
    public void testInsert() {
        assertThrows(CJException.class, () -> INSTANCE.insert(RW, URL_RW),
                "INSERT command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
    }

    @Test
    @Order(6)
    public void testUpdate() {
        assertThrows(CJException.class, () -> INSTANCE.update(RW, URL_RW),
                "UPDATE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
    }
    @Test
    @Order(7)
    public void testSelect() {
        assertDoesNotThrow(() -> INSTANCE.select(RO, URL_RW));
    }

    @Test
    @Order(8)
    public void testDelete() {
        assertThrows(CJException.class, () -> INSTANCE.delete(RW, URL_RW),
                "DELETE command denied to user 'test_ro'@'10.22.87.7' for table 'jdbc_direct_tb'");
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



    private static final WriteAndReadTest INSTANCE = new WriteAndReadTest();

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
    public static final String SELECT = "SELECT * FROM `jdbc_direct_db`.`jdbc_direct_tb` ORDER BY `id`";
}
