package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class getDelayConnection {
    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    protected static final String URL = "jdbc:tdsql-mysql:direct://9.30.1.207:15006,9.30.1.231:15006" +
            "/test?useLocalSessionStates=true&useUnicode=true&" +
            "characterEncoding=utf-8&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro&" +
            "tdsqlDirectMaxSlaveDelaySeconds=100&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&" +
            "tdsqlDirectCloseConnTimeoutMillis=500&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=Sed";
    protected static final String USER = "tdsqlsys_normal";
    protected static final String PASS = "5R77aqf9kSk8HnN%R";
    public static final String DROP_DATABASE_IF_EXISTS = "DROP DATABASE IF EXISTS `jdbc_direct_db`";
    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Test
    public  void getConnection(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(conn);
    }
}
