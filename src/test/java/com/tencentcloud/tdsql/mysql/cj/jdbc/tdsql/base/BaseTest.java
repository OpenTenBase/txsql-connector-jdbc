package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.base;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectReadWriteMode.RW;

public abstract class BaseTest {
    /**
     *1、 9.30.1.231:15050； user：tdsqlsys_normal   pwd：gl%LDY^1&OKWkLWQP^7&
     *2、 9.30.1.207:15050；  user：tdsqlsys_normal  pwd：gl%LDY^1&OKWkLWQP^7&
     *3、 9.135.135.186:3306
     *
     * private static final String USERNAME = "tdsql_admin";
     *     private static final String PASSWORD = "Mkhdb*8532XucF";
     */
    protected static final String DRIVER_CLASS_NAME = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    protected static final String URL_RW = "jdbc:tdsql-mysql:direct://"
            + "9.30.1.231:15050,"
            + "9.30.1.178:4015,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=rw";
    protected static final String URL_RO = "jdbc:tdsql-mysql:direct://"
            + "9.30.1.231:15050,"
            + "9.30.1.178:4015,"
            + "/mysql?useSSL=false&tdsqlReadWriteMode=ro&tdsqlMaxSlaveDelay=12.9";
    protected static final String USER = "tdsqlsys_normal";
//    protected static final String USER = "gyokumeixie";
//    protected static final String USER = "tdsql_admin";
    protected static final String PASS = "gl%LDY^1&OKWkLWQP^7&";
//    protected static final String PASS = "H3<2m6_7F0B+y5^7";
    // H3<2m6_7F0B+y5^7
//    protected static final String PASS = "Mkhdb*8532XucF";

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.printf("Running test: %s, method: %s%n",
                testInfo.getTestClass().orElse(Class.forName("java.lang.NullPointerException")).getName(),
                testInfo.getDisplayName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        Class.forName(DRIVER_CLASS_NAME);
    }

    protected Connection getConnection(TdsqlDirectReadWriteMode mode, String user) throws SQLException {
        Properties props = new Properties();
        if (USER.equalsIgnoreCase(user)) {
            props.setProperty(PropertyKey.USER.getKeyName(), USER);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS);
        } else {
            props.setProperty(PropertyKey.USER.getKeyName(), USER);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS);
        }
        return getConnection(mode, props);
    }

    protected Connection getConnection(TdsqlDirectReadWriteMode mode, Properties properties) throws SQLException {
        Properties props = new Properties();
        if (RO.equals(mode)) {
            props.setProperty(PropertyKey.USER.getKeyName(), USER);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS);
            props.setProperty(PropertyKey.tdsqlDirectReadWriteMode.getKeyName(), RO.toString());
            props.setProperty(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName(), "true");
        } else {
            props.setProperty(PropertyKey.USER.getKeyName(), USER);
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), PASS);
            props.setProperty(PropertyKey.tdsqlDirectReadWriteMode.getKeyName(), RW.toString());
        }
        props.putAll(properties);
        return DriverManager.getConnection(URL_RW, props);
    }
}
