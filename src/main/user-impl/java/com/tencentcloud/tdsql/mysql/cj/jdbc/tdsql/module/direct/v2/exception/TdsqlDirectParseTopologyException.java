package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式解析拓扑信息异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectParseTopologyException extends TdsqlException {

    private static final long serialVersionUID = 3367828519878593985L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectParseTopologyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectParseTopologyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectParseTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectParseTopologyException(Throwable cause) {
        super(cause);
    }
}
