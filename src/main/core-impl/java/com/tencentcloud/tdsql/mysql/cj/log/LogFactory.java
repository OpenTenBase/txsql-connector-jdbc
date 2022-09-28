/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.tencentcloud.tdsql.mysql.cj.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.tencentcloud.tdsql.mysql.cj.exceptions.ExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.exceptions.WrongArgumentException;
import com.tencentcloud.tdsql.mysql.cj.util.Util;

/**
 * Creates instances of loggers for the driver to use.
 */
public class LogFactory {
    private static Constructor logConstructor;

    /**
     * Returns a logger instance of the given class, with the given instance
     * name.
     * 
     * @param className
     *            the class to instantiate
     * @param instanceName
     *            the instance name
     * @return a logger instance
     */
    public static Log getLogger(String className, String instanceName) {

        if (className == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Logger class can not be NULL");
        }

        if (instanceName == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Logger instance name can not be NULL");
        }

        try {
            Class<?> loggerClass = null;

            try {
                loggerClass = Class.forName(className);
            } catch (ClassNotFoundException nfe) {
                loggerClass = Class.forName(Util.getPackageName(LogFactory.class) + "." + className);
            }

            Constructor<?> constructor = loggerClass.getConstructor(new Class<?>[] { String.class });

            return (Log) constructor.newInstance(new Object[] { instanceName });
        } catch (ClassNotFoundException cnfe) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Unable to load class for logger '" + className + "'", cnfe);
        } catch (NoSuchMethodException nsme) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class does not have a single-arg constructor that takes an instance name", nsme);
        } catch (InstantiationException inse) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', exception in constructor?", inse);
        } catch (InvocationTargetException ite) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', exception in constructor?", ite);
        } catch (IllegalAccessException iae) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', constructor not public", iae);
        } catch (ClassCastException cce) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class '" + className + "' does not implement the '" + Log.class.getName() + "' interface", cce);
        }
    }

//    public static Log getLog(Class clazz) {
//        return getLog(clazz.getName());
//    }

    public static Log getLog(String loggerName) {
        try {
            return (Log) logConstructor.newInstance(loggerName);
        } catch (Throwable t) {
            throw new RuntimeException("Error creating logger for logger '" + loggerName + "'.  Cause: " + t, t);
        }
    }

    public static Constructor setAndGetLogConstructor(String logType){
        if (logType != null) {
            if (logType.equalsIgnoreCase("Slf4JLogger")) {
                tryImplementation("org.slf4j.Logger", "com.tencentcloud.tdsql.mysql.cj.log.Slf4JLogger");
            } else if (logType.equalsIgnoreCase("log4j")) {
                tryImplementation("org.apache.log4j.Logger", "com.jd.jdbc.sqlparser.support.logging.Log4jImpl");
            } else if (logType.equalsIgnoreCase("log4j2")) {
                tryImplementation("org.apache.logging.log4j.Logger", "com.jd.jdbc.sqlparser.support.logging.Log4j2Impl");
            } else if (logType.equalsIgnoreCase("commonsLog")) {
                tryImplementation("org.apache.commons.logging.LogFactory",
                        "com.jd.jdbc.sqlparser.support.logging.JakartaCommonsLoggingImpl");
            } else if (logType.equalsIgnoreCase("jdkLog")) {
                tryImplementation("java.util.logging.Logger", "com.jd.jdbc.sqlparser.support.logging.Jdk14LoggingImpl");
            }
        }
        // 优先选择log4j,而非Apache Common Logging. 因为后者无法设置真实Log调用者的信息
        tryImplementation("org.slf4j.Logger", "com.jd.jdbc.sqlparser.support.logging.SLF4JImpl");
        tryImplementation("org.apache.log4j.Logger", "com.jd.jdbc.sqlparser.support.logging.Log4jImpl");
        tryImplementation("org.apache.logging.log4j.Logger", "com.jd.jdbc.sqlparser.support.logging.Log4j2Impl");
        tryImplementation("org.apache.commons.logging.LogFactory",
                "com.jd.jdbc.sqlparser.support.logging.JakartaCommonsLoggingImpl");
        tryImplementation("java.util.logging.Logger", "com.jd.jdbc.sqlparser.support.logging.Jdk14LoggingImpl");

        if (logConstructor == null) {
            try {
//                logConstructor = NoLoggingImpl.class.getConstructor(String.class);
                logConstructor = NullLogger.class.getConstructor(String.class);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return logConstructor;
    }

    private static void tryImplementation(String testClassName, String implClassName) {
        if (logConstructor != null) {
            return;
        }

        try {
            Resources.classForName(testClassName);
            Class implClass = Resources.classForName(implClassName);
            logConstructor = implClass.getConstructor(new Class[]{String.class});

            Class<?> declareClass = logConstructor.getDeclaringClass();
            if (!Log.class.isAssignableFrom(declareClass)) {
                logConstructor = null;
            }

            try {
                if (null != logConstructor) {
                    logConstructor.newInstance(LogFactory.class.getName());
                }
            } catch (Throwable t) {
                logConstructor = null;
            }

        } catch (Throwable t) {
            // skip
        }
    }
}
