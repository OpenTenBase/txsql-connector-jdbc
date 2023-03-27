package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式处理故障转移异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectHandleFailoverException extends TdsqlException {

    private static final long serialVersionUID = 3015090342476046460L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectHandleFailoverException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectHandleFailoverException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectHandleFailoverException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectHandleFailoverException(Throwable cause) {
        super(cause);
    }
}
