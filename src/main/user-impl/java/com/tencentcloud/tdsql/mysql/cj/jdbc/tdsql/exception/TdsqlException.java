package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception;

/**
 * <p>TDSQL专属异常类，继承自{@link RuntimeException}</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlException extends RuntimeException {

    private static final long serialVersionUID = -2825530029440782520L;

    /**
     * {@inheritDoc}
     */
    public TdsqlException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlException(Throwable cause) {
        super(cause);
    }
}
