package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.exception;

import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;

import java.sql.SQLException;

public class TdsqlRouteParseException extends SQLException {

    public TdsqlRouteParseException(String reason) {
        super(reason, MysqlErrorNumbers.SQL_STATE_ERROR_IN_ROW);
    }
}
