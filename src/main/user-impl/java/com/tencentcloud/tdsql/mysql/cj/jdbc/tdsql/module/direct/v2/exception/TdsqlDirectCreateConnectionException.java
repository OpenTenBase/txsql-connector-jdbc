package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式创建连接异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectCreateConnectionException extends TdsqlException {

    private static final long serialVersionUID = -826682898128171580L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCreateConnectionException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCreateConnectionException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCreateConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCreateConnectionException(Throwable cause) {
        super(cause);
    }
}
