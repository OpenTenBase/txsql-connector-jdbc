package com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions;

import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;

import java.sql.SQLException;

public class TDSQLRouteParseException extends SQLException {

    public TDSQLRouteParseException(String reason) {
        super(reason, MysqlErrorNumbers.SQL_STATE_ERROR_IN_ROW);
    }
}
