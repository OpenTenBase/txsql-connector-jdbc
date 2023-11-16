package tdsql.direct.v2.close;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestConnectionClose extends TdsqlDirectBaseTest {
    protected static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    protected static final String URL_RW = "jdbc:tdsql-mysql:direct://"
            + PROXY_1 + "," + PROXY_2 + "," + PROXY_3
            + "/test?useSSL=false&tdsqlReadWriteMode=rw";
    protected static final String URL_RO = "jdbc:tdsql-mysql:direct://"
            + PROXY_1 + "," + PROXY_2 + "," + PROXY_3
            + "/test?useSSL=false&tdsqlReadWriteMode=ro&tdsqlMaxSlaveDelay=50";
    protected static final String USER_RO = "qt4s_ro";
    protected static final String PASS_RO = "g<m:7KNDF.L1<^1C";

    @BeforeAll
    public static void init() throws ClassNotFoundException {
        Class.forName(DRIVER_CLASS_NAME);
    }

    @Test
    public void testParamValueThreshold() {
        String urlWithValueOverThreshold = URL_RW + "&tdsqlDirectProxyConnectMaxIdleTime=3601";
        try (Connection conn = DriverManager.getConnection(urlWithValueOverThreshold, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
            Assertions.fail("Connection should not be created, as param value is greater than threshold");
        } catch (RuntimeException e) {
            // do Nothing
        } catch (SQLException e) {
            Assertions.fail("Exception is not right");
        }

        String urlWithValueUnderThreshold = URL_RW + "&tdsqlDirectProxyConnectMaxIdleTime=29";
        try (Connection conn = DriverManager.getConnection(urlWithValueUnderThreshold, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
            Assertions.fail("Connection should not be created, as param value is lower than threshold");
        } catch (RuntimeException e) {
            // do Nothing
        } catch (SQLException e) {
            Assertions.fail("Exception is not right");
        }
    }
    @Test
    public void testCloseConnection() throws InterruptedException {
        String url = URL_RO + "&tdsqlDirectProxyConnectMaxIdleTime=40";
        try (Connection conn = DriverManager.getConnection(url, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(50000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count == 0);
    }

    @Test
    public void testCloseAndRebuildConnection() throws InterruptedException {
        String url = URL_RO + "&tdsqlDirectProxyConnectMaxIdleTime=40";
        try (Connection conn = DriverManager.getConnection(url, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(50000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertEquals(0, count);
        try (Connection conn = DriverManager.getConnection(url, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
    }

    @Test
    public void testCloseAndRebuildBeforeCloseProxyConnection() throws InterruptedException {
        String url = URL_RO + "&tdsqlDirectProxyConnectMaxIdleTime=40";
        try (Connection conn = DriverManager.getConnection(url, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(30000);
        int count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
        try (Connection conn = DriverManager.getConnection(url, USER, PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count >=1);
        Thread.sleep(50000);
        count = Integer.parseInt(executeCommand("netstat -an |grep " + getPort() + "|grep  ESTABLISHED |wc -l").trim());
        Assertions.assertTrue(count == 0);
    }

    @Test
    public void testCloseProxyConnectionWithWrongConnectionInfo() throws InterruptedException {
        String url = URL_RO + "&tdsqlDirectProxyConnectMaxIdleTime=40&connectTimeout=3000";
        try (Connection conn = DriverManager.getConnection(url, USER, "wrong_pass");
             Statement stmt = conn.createStatement()) {
            stmt.execute("select 1");
        } catch (Throwable e) {
            // do nothing
        }
        Thread.sleep(3000 * 6);
        Assertions.assertEquals(0, countTopoRefreshThreads());
    }

    public static int countTopoRefreshThreads() {
        int count = 0;
        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        //activeCount()返回当前正在活动的线程的数量
        int total = Thread.activeCount();
        Thread[] threads = new Thread[total];
        //enumerate(threads)将当前线程组中的active线程全部复制到传入的线程数组threads中
        // 并且返回数组中元素个数，即线程组中active线程数量
        threadGroup.enumerate(threads);
        for (Thread t:threads){
            if (t.getName().startsWith("TopologyRefresh-")) {
                count++;
            }
        }
        return count;
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
