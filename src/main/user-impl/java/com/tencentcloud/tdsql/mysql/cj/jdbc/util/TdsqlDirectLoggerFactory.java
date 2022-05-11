package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

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

    public static Log getLogger() {
        return TdsqlDirectLoggerFactory.log;
    }
}
