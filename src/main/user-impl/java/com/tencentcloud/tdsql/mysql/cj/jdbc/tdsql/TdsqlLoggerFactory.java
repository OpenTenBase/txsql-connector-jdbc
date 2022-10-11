package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertySet;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.log.Log;
import com.tencentcloud.tdsql.mysql.cj.log.LogFactory;

import java.lang.reflect.Constructor;
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

    public static void setLogger(Log log) {
        TdsqlLoggerFactory.log = log;
    }

    public static void setLogger(TdsqlHostInfo tdsqlHostInfo) {
        Properties properties = tdsqlHostInfo.exposeAsProperties();
        properties.remove(PropertyKey.tdsqlLoadBalanceStrategy.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceWeightFactor.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatMonitorEnable.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatIntervalTimeMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatMaxErrorRetries.getKeyName());
        properties.remove(PropertyKey.tdsqlLoadBalanceHeartbeatErrorRetryIntervalTimeMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectReadWriteMode.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectMaxSlaveDelaySeconds.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectTopoRefreshIntervalMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectTopoRefreshConnTimeoutMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectTopoRefreshStmtTimeoutSeconds.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectCloseConnTimeoutMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectMasterCarryOptOfReadOnlyMode.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectHeartbeatMonitorEnable.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectHeartbeatIntervalTimeMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectHeartbeatMaxErrorRetries.getKeyName());
        properties.remove(PropertyKey.tdsqlDirectHeartbeatErrorRetryIntervalTimeMillis.getKeyName());
        properties.remove(PropertyKey.tdsqlQueryAttributesEnable.getKeyName());
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
