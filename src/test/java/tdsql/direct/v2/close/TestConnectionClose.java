package tdsql.direct.v2.close;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestConnectionClose {
    protected static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    protected static final String URL_RW = "jdbc:tdsql-mysql:direct://"
            + "9.30.2.116:15012,9.30.2.89:15016"
            + "/test?useSSL=false&tdsqlReadWriteMode=rw";
    protected static final String URL_RO = "jdbc:tdsql-mysql:direct://"
            + "9.30.2.116:15012,9.30.2.89:15016"
            + "/test?useSSL=false&tdsqlReadWriteMode=ro&tdsqlMaxSlaveDelay=12&tdsqlDirectProxyConnectMaxIdleTime=3601";
    protected static final String USER_RW = "qt4s";
    protected static final String PASS_RW = "g<m:7KNDF.L1<^1C";
    protected static final String USER_RO = "qt4s_ro";
    protected static final String PASS_RO = "g<m:7KNDF.L1<^1C";

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DRIVER_CLASS_NAME);
    }
    @Test
    public void testCloseConnection() throws InterruptedException {
        try (Connection conn = DriverManager.getConnection(URL_RO, USER_RW, PASS_RW);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(63000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep 15016|grep  ESTABLISHED |wc -l").trim());
        System.out.println("count:" + count);
    }

    @Test
    public void testCloseAndRebuildConnection() throws InterruptedException {
        try (Connection conn = DriverManager.getConnection(URL_RO, USER_RW, PASS_RW);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(43000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep 15016|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertEquals(0, count);
        try (Connection conn = DriverManager.getConnection(URL_RO, USER_RW, PASS_RW);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        count = Integer.parseInt(executeCommand("netstat -an |grep 15016|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
    }

    @Test
    public void testCloseAndRebuildBeforeCloseProxyConnection() throws InterruptedException {
        try (Connection conn = DriverManager.getConnection(URL_RO, USER_RW, PASS_RW);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(23000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep 15016|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
        try (Connection conn = DriverManager.getConnection(URL_RO, USER_RW, PASS_RW);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        count = Integer.parseInt(executeCommand("netstat -an |grep 15016|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
        Thread.sleep(25000);
    }

    public static String executeCommand(String command) {
        try {
            // 构建命令
            Process process = new ProcessBuilder("sh", "-c", command).start();

            // 读取命令输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            // 等待命令执行完成
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Command exited with code " + exitCode);
            }
            String result = output.toString();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
