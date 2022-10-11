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

import com.tencentcloud.tdsql.mysql.cj.exceptions.ExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.exceptions.WrongArgumentException;
import com.tencentcloud.tdsql.mysql.cj.util.Util;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Creates instances of loggers for the driver to use.
 */
public class LogFactory {

    /**
     * Returns a logger instance of the given class, with the given instance
     * name.
     *
     * @param className the class to instantiate
     * @param instanceName the instance name
     * @return a logger instance
     */
    public static Log getLogger(String className, String instanceName) {

        if (className == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "Logger class can not be NULL");
        }

        if (instanceName == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger instance name can not be NULL");
        }

        try {
            Class<?> loggerClass;

            try {
                loggerClass = Class.forName(className);
            } catch (ClassNotFoundException nfe) {
                loggerClass = Class.forName(Util.getPackageName(LogFactory.class) + "." + className);
            }

            Constructor<?> constructor = loggerClass.getConstructor(String.class);

            return (Log) constructor.newInstance(new Object[]{instanceName});
        } catch (ClassNotFoundException e1) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to load class for logger '" + className + "'", e1);
        } catch (NoSuchMethodException e2) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class does not have a single-arg constructor that takes an instance name", e2);
        } catch (InstantiationException | InvocationTargetException inse) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', exception in constructor?", inse);
        } catch (IllegalAccessException e3) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Unable to instantiate logger class '" + className + "', constructor not public", e3);
        } catch (ClassCastException e4) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "Logger class '" + className + "' does not implement the '" + Log.class.getName() + "' interface",
                    e4);
        }
    }
}
