package tdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CharacterSetResultTest {

    public static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
            String proxyUrl = "jdbc:tdsql-mysql://9.135.146.159:15002/db20211125?useConfigs=maxPerformance&useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&allowMultiQueries=true";
            String dbUrl = "jdbc:tdsql-mysql://9.135.144.125:4002/db20211125?useConfigs=maxPerformance&useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&allowMultiQueries=true";
            try {
                conn = DriverManager.getConnection(proxyUrl, "test", "123456");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }


    public static void main(String[] args) {
        String sql1 = " select  c.uuid, c.CJMBUUID,   a.CJMBMC,   c.FJYSXMUUID,   b.FJYSXMMC \n" +
                " from   dzfp_fpfjyscjmbxxb a,   dzfp_jcxx_fpfjysxxb b,   dzfp_kp_fiyscjmbmapper c  \n" +
                " where   a.UUID = c.CJMBUUID   and b.UUID = c.FJYSXMUUID   \n" +
                " and c.NSRSBH='bbbb'      \n" +
                " and a.YXBZ='1'   and b.YXBZ='1'   and c.YXBZ='1';";
        String sql2 = " select  c.uuid, c.CJMBUUID,   a.CJMBMC,   c.FJYSXMUUID,   b.FJYSXMMC \n" +
                " from   dzfp_fpfjyscjmbxxb a,   dzfp_jcxx_fpfjysxxb1 b,   dzfp_kp_fiyscjmbmapper c  \n" +
                " where   a.UUID = c.CJMBUUID   and b.UUID = c.FJYSXMUUID   \n" +
                " and c.NSRSBH='bbbb'      \n" +
                " and a.YXBZ='1'   and b.YXBZ='1'   and c.YXBZ='1';";
        String sql3 = " select  c.uuid, c.CJMBUUID,   a.CJMBMC,   c.FJYSXMUUID,   b.FJYSXMMC \n" +
                " from   dzfp_fpfjyscjmbxxb a,   dzfp_jcxx_fpfjysxxb2 b,   dzfp_kp_fiyscjmbmapper c  \n" +
                " where   a.UUID = c.CJMBUUID   and b.UUID = c.FJYSXMUUID   \n" +
                " and c.NSRSBH='bbbb'      \n" +
                " and a.YXBZ='1'   and b.YXBZ='1'   and c.YXBZ='1';";
        ResultSet rs = null;
        Connection conn = getConn();

        try {
            PreparedStatement stmt1 = conn.prepareStatement(sql1);
//            System.out.println("查询:\n" + sql1);
            rs = stmt1.executeQuery();
            while (rs.next()) {
                System.out.println(
                        "uuid:" + rs.getString(1) + ", cjmbuuid:" + rs.getString(2) + ", cjmbmc:" + rs.getString(3)
                                + ", FJYSXMUUID:" + rs.getString(4) + ", FJYSXMMC:" + rs.getString(5));
            }
            PreparedStatement stmt2 = conn.prepareStatement(sql2);
//            System.out.println("查询:\n" + sql2);
            rs = stmt2.executeQuery();
            while (rs.next()) {
                System.out.println(
                        "uuid:" + rs.getString(1) + ", cjmbuuid:" + rs.getString(2) + ", cjmbmc:" + rs.getString(3)
                                + ", FJYSXMUUID:" + rs.getString(4) + ", FJYSXMMC:" + rs.getString(5));
            }
            PreparedStatement stmt3 = conn.prepareStatement(sql3);
//            System.out.println("查询:\n" + sql3);
            rs = stmt3.executeQuery();
            while (rs.next()) {
                System.out.println(
                        "uuid:" + rs.getString(1) + ", cjmbuuid:" + rs.getString(2) + ", cjmbmc:" + rs.getString(3)
                                + ", FJYSXMUUID:" + rs.getString(4) + ", FJYSXMMC:" + rs.getString(5));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
