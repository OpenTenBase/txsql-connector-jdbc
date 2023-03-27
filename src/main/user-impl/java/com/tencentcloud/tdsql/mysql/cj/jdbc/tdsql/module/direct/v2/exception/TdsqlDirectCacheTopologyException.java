package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式缓存拓扑信息异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectCacheTopologyException extends TdsqlException {

    private static final long serialVersionUID = 6339619896622867665L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCacheTopologyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCacheTopologyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCacheTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectCacheTopologyException(Throwable cause) {
        super(cause);
    }
}
