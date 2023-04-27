package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlUnImplementMethodException extends TdsqlException {

    private static final long serialVersionUID = 297091414295740304L;

    /**
     * {@inheritDoc}
     */
    public TdsqlUnImplementMethodException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlUnImplementMethodException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlUnImplementMethodException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlUnImplementMethodException(Throwable cause) {
        super(cause);
    }
}
