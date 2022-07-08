package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.exception;

import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;

import java.sql.SQLException;

public class TdsqlNoBackendInstanceException extends SQLException {

    public TdsqlNoBackendInstanceException(String reason) {
        super(reason, MysqlErrorNumbers.SQL_STATE_ERROR_IN_ROW);
    }
}
