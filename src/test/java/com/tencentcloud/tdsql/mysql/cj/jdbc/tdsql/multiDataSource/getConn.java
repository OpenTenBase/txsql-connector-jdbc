package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.multiDataSource;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class getConn {
    private static final String DRIVER_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:tdsql-mysql:direct://9.30.1.207:15006,9.30.1.231:15006/test?useLocalSessionStates=true" +
            "&useUnicode=true&characterEncoding=utf-8" +
            "&serverTimezone=Asia/Shanghai&tdsqlDirectReadWriteMode=ro" +
            "&tdsqlDirectMaxSlaveDelaySeconds=50" +
            "&tdsqlDirectTopoRefreshIntervalMillis=500&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
            "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=1&tdsqlDirectCloseConnTimeoutMillis=500" +
            "&tdsqlDirectMasterCarryOptOfReadOnlyMode=true&tdsqlLoadBalanceStrategy=Sed";
    private static final String USERNAME = "tdsqlsys_normal";
    private static final String PASSWORD = "5R77aqf9kSk8HnN%R";

    @Test
    public void get() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_NAME);
        Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
        System.out.println(connection);
    }
}
