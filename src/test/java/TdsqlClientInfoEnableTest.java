import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TdsqlClientInfoEnableTest {
    private static final String URL = "jdbc:tdsql-mysql:direct://9.30.2.89:15018/test?passwordCharacterEncoding=latin1&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useUnicode=true&useSSL=false&connectTimeout=10000&socketTimeout=600000&allowMultiQueries=true&tdsqlSendClientInfoEnable=true";
    private static final String USER = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Connection conn1 = DriverManager.getConnection(URL, USER, PASSWORD);){
            System.out.println(conn);
        }
    }
}
