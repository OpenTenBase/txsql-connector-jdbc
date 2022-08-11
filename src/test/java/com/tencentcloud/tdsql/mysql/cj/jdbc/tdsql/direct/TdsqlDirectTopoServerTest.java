package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcPropertySetImpl;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TdsqlDirectTopoServerTest {
    @Test
    public void TestRo(){
        String url = "jdbc:tdsql-mysql:direct://"
                + "9.30.1.231:15006,"
                + "/mysql?useSSL=false&tdsqlDirectReadWriteMode=ro&tdsqlLoadBalanceStrategy=Sed&tdsqlDirectMasterCarryOptOfReadOnlyMode=true";
        Properties properties = new Properties();
        ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(url, properties);
        System.out.println(connectionUrl);
        JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
        connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

        String newTdsqlReadWriteMode = connProps.getStringProperty(PropertyKey.tdsqlDirectReadWriteMode).getValue();
        System.out.println(newTdsqlReadWriteMode);
    }

}