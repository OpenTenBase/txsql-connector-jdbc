package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;

/**
 * <p>TDSQL专属异常工厂类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlExceptionFactory {

    @SuppressWarnings("unchecked")
    public static <T extends TdsqlException> T createException(Class<T> clazz, String message) {
        T ex;
        try {
            ex = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            ex = (T) new TdsqlException(message);
        }
        return ex;
    }

    @SuppressWarnings("unchecked")
    public static <T extends TdsqlException> T logException(String datasourceUuid, Class<T> clazz, String message) {
        T ex;
        try {
            ex = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            ex = (T) new TdsqlException(message);
        } finally {
            TdsqlLoggerFactory.logError(datasourceUuid, message);
        }
        return ex;
    }

    @SuppressWarnings("unchecked")
    public static <T extends TdsqlException> T logException(Class<T> clazz, String message) {
        T ex;
        try {
            ex = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            ex = (T) new TdsqlException(message);
        } finally {
            TdsqlLoggerFactory.logError(message);
        }
        return ex;
    }
}
