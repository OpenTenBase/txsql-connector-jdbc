package tdsql;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CharacterEncodingTest {

    private static final String URL = "jdbc:tdsql-mysql://9.30.2.94:15003/test?characterEncoding=UTF-8";
    private static final String USERNAME = "test3";
    private static final String PASSWORD = "test3";

    @BeforeAll
    public static void setUp() {
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("truncate table tt1_hash");

            for (int i = 1; i <= 100; i++) {
                String chinese = getChinese();
                stmt.executeUpdate("insert into tt1_hash(`id`, `name`) values (" + i + ", '" + chinese + "');");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void case00() {
        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select t1.`name` from tt1_hash t1 left join tt1 t2 on t1.id = t2.id order by t1.`id`")) {
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getChinese() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 2; i++) {
            String str = "";
            int highPos;
            int lowPos;
            Random random = new Random();

            highPos = (176 + Math.abs(random.nextInt(39)));
            lowPos = (161 + Math.abs(random.nextInt(93)));

            byte[] b = new byte[2];
            b[0] = (Integer.valueOf(highPos)).byteValue();
            b[1] = (Integer.valueOf(lowPos)).byteValue();

            try {
                str = new String(b, "GBK");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            s.append(str.charAt(0));
        }
        return s.toString();
    }
}
