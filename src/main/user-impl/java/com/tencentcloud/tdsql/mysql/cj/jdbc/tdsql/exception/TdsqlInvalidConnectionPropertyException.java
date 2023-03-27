package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception;

/**
 * <p>TDSQL专属异常类，当连接属性无效时抛出</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlInvalidConnectionPropertyException extends TdsqlException {

    private static final long serialVersionUID = -6440154230661606883L;

    /**
     * {@inheritDoc}
     */
    public TdsqlInvalidConnectionPropertyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlInvalidConnectionPropertyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlInvalidConnectionPropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlInvalidConnectionPropertyException(Throwable cause) {
        super(cause);
    }
}
