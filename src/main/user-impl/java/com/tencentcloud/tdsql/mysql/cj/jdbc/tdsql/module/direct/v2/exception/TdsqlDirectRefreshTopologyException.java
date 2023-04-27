package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式刷新拓扑信息异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectRefreshTopologyException extends TdsqlException {

    private static final long serialVersionUID = -2004121253282500097L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectRefreshTopologyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectRefreshTopologyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectRefreshTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectRefreshTopologyException(Throwable cause) {
        super(cause);
    }
}
