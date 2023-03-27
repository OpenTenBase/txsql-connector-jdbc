package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlException;

/**
 * <p>TDSQL专属，直连模式调度拓扑信息异常类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectScheduleTopologyException extends TdsqlException {

    private static final long serialVersionUID = 8430060240714943287L;

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectScheduleTopologyException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectScheduleTopologyException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectScheduleTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@inheritDoc}
     */
    public TdsqlDirectScheduleTopologyException(Throwable cause) {
        super(cause);
    }
}
