package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式数据源异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectDataSourceException extends TdsqlException {

    private static final long serialVersionUID = -3721395496470978526L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectDataSourceException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectDataSourceException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectDataSourceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectDataSourceException(Throwable cause) {
        super(cause);
    }
}
