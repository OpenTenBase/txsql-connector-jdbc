package com.tencentcloud.tdsql.mysql.cj.log;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Log4JLogger implements Log{
    private Logger log;
    public Log4JLogger(String name) {
        this.log = LogManager.getLogger(name);
    }

    public boolean isDebugEnabled() {
        return this.log.isDebugEnabled();
    }

    public boolean isErrorEnabled() {
        return this.log.isEnabledFor(Level.ERROR);
    }

    public boolean isFatalEnabled() {
        return this.log.isEnabledFor(Level.FATAL);
    }

    public boolean isInfoEnabled() {
        return this.log.isInfoEnabled();
    }

    public boolean isTraceEnabled() {
        return this.log.isTraceEnabled();
    }

    public boolean isWarnEnabled() {
        return this.log.isEnabledFor(Level.WARN);
    }

    public void logDebug(Object msg) {
        this.log.debug(msg.toString());
    }

    public void logDebug(Object msg, Throwable thrown) {
        this.log.debug(msg.toString(), thrown);
    }

    public void logError(Object msg) {
        this.log.error(msg.toString());
    }

    public void logError(Object msg, Throwable thrown) {
        this.log.error(msg.toString(), thrown);
    }

    public void logFatal(Object msg) {
        this.log.error(msg.toString());
    }

    public void logFatal(Object msg, Throwable thrown) {
        this.log.error(msg.toString(), thrown);
    }

    public void logInfo(Object msg) {
        this.log.info(msg.toString());
    }

    public void logInfo(Object msg, Throwable thrown) {
        this.log.info(msg.toString(), thrown);
    }

    public void logTrace(Object msg) {
        this.log.trace(msg.toString());
    }

    public void logTrace(Object msg, Throwable thrown) {
        this.log.trace(msg.toString(), thrown);
    }

    public void logWarn(Object msg) {
        this.log.warn(msg.toString());
    }

    public void logWarn(Object msg, Throwable thrown) {
        this.log.warn(msg.toString(), thrown);
    }
}
