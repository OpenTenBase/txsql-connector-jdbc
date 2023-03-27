package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.base.BaseTest;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class URLTest extends BaseTest {

    @Test
    public void TestUrl() throws SQLException {

        for (int i = 0; i < 10; i++) {
            if (i == 9){
                System.out.println();
            }
            Connection connection = getConnection(TdsqlDirectReadWriteModeEnum.RO, URL_RO);
            System.out.println(connection);
        }
    }
    public class GetConnection implements Runnable{
        @Override
        public void run() {
            try
            {
                Connection connection = getConnection(TdsqlDirectReadWriteModeEnum.RO, URL_RO);
                System.out.println(Thread.currentThread().getName() + ":" + connection);
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
