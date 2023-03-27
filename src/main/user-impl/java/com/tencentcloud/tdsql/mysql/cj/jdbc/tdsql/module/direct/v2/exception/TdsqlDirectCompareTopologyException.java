package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式比较拓扑信息异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectCompareTopologyException extends TdsqlException {

    private static final long serialVersionUID = -8268638364680020864L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCompareTopologyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCompareTopologyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCompareTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCompareTopologyException(Throwable cause) {
        super(cause);
    }
}
