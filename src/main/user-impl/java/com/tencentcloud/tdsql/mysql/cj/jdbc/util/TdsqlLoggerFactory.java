package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertySet;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.tencentcloud.tdsql.mysql.cj.log.Log;
import com.tencentcloud.tdsql.mysql.cj.log.LogFactory;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlLoggerFactory {

    public static final AtomicBoolean loggerInitialized = new AtomicBoolean(false);
    private static Log log;

    private TdsqlLoggerFactory() {
    }

    public static void setLogger(TdsqlHostInfo tdsqlHostInfo) {
        Properties properties = tdsqlHostInfo.exposeAsProperties();
        properties.remove(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceWeightFactor.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceBlacklistTimeoutMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatMonitorEnable.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatIntervalTimeMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatMaxErrorRetries.getKeyName());
        PropertySet propertySet = new JdbcPropertySetImpl();
        propertySet.initializeProperties(properties);
        log = LogFactory.getLogger(propertySet.getStringProperty(PropertyKey.logger).getStringValue(),
                Log.LOGGER_INSTANCE_NAME);
    }

    public static void logDebug(Object msg) {
        if (log != null && log.isDebugEnabled()) {
            log.logDebug(printThreadId() + msg);
        }
    }

    public static void logInfo(Object msg) {
        if (log != null && log.isInfoEnabled()) {
            log.logInfo(printThreadId() + msg);
        }
    }

    public static void logWarn(Object msg) {
        if (log != null && log.isWarnEnabled()) {
            log.logWarn(msg);
        }
    }

    public static void logError(Object msg) {
        if (log != null && log.isErrorEnabled()) {
            log.logError(printThreadId() + msg);
        }
    }

    public static void logError(Object msg, Throwable thrown) {
        if (log != null && log.isErrorEnabled()) {
            log.logError(printThreadId() + msg, thrown);
        }
    }

    public static void logFatal(Object msg) {
        if (log != null && log.isFatalEnabled()) {
            log.logFatal(printThreadId() + msg);
        }
    }

    private static String printThreadId() {
        return "[TID: " + Thread.currentThread().getId() + "] ";
    }
}
