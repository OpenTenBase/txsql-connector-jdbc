/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencentcloud.tdsql.mysql.cj.log;

public class NoLoggingImpl implements Log {
    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isFatalEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void logDebug(Object msg) {

    }

    @Override
    public void logDebug(Object msg, Throwable thrown) {

    }

    @Override
    public void logError(Object msg) {

    }

    @Override
    public void logError(Object msg, Throwable thrown) {

    }

    @Override
    public void logFatal(Object msg) {

    }

    @Override
    public void logFatal(Object msg, Throwable thrown) {

    }

    @Override
    public void logInfo(Object msg) {

    }

    @Override
    public void logInfo(Object msg, Throwable thrown) {

    }

    @Override
    public void logTrace(Object msg) {

    }

    @Override
    public void logTrace(Object msg, Throwable thrown) {

    }

    @Override
    public void logWarn(Object msg) {

    }

    @Override
    public void logWarn(Object msg, Throwable thrown) {

    }
}
