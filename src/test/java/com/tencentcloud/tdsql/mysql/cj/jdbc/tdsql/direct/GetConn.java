package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.base.BaseTest;
import org.junit.jupiter.api.Test;

import java.sql.*;

public class GetConn extends BaseTest {
    @Test
    public void TestConn(){
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(URL_RO, USER, PASS);
            System.out.println("get:" + conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
