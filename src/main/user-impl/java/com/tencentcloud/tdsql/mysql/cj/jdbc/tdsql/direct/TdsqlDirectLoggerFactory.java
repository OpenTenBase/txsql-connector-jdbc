package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.log.Log;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectLoggerFactory {

    private static Log log;

    private TdsqlDirectLoggerFactory() {
    }

    public static void setLogger(Log log) {
        TdsqlDirectLoggerFactory.log = log;
    }

    public static void logDebug(Object msg) {
        if (log != null && log.isDebugEnabled()) {
            log.logDebug(msg);
        }
    }

    public static void logInfo(Object msg) {
        if (log != null && log.isInfoEnabled()) {
            log.logInfo(msg);
        }
    }

    public static void logError(Object msg) {
        if (log != null && log.isErrorEnabled()) {
            log.logError(msg);
        }
    }

    public static void logError(Object msg, Throwable thrown) {
        if (log != null && log.isErrorEnabled()) {
            log.logError(msg, thrown);
        }
    }
}
