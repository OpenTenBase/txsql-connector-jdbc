package com.tencentcloud.tdsql.mysql.cj.log;

import org.apache.commons.logging.LogFactory;


public class CommonsLoggingLogger implements Log {
    private org.apache.commons.logging.Log log;

    public CommonsLoggingLogger(String name){
        this.log = LogFactory.getLog(name);
    }

    @Override
    public boolean isDebugEnabled() {
        return this.log.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return this.log.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled() {
        return this.log.isFatalEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return this.log.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return this.log.isTraceEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return this.log.isWarnEnabled();
    }

    @Override
    public void logDebug(Object msg) {
        this.log.debug(msg.toString());
    }

    @Override
    public void logDebug(Object msg, Throwable thrown) {
        this.log.debug(msg.toString(), thrown);
    }

    @Override
    public void logError(Object msg) {
        this.log.error(msg.toString());
    }

    @Override
    public void logError(Object msg, Throwable thrown) {
        this.log.error(msg.toString(), thrown);
    }

    @Override
    public void logFatal(Object msg) {
        this.log.fatal(msg.toString());
    }

    @Override
    public void logFatal(Object msg, Throwable thrown) {
        this.log.fatal(msg.toString(), thrown);
    }

    @Override
    public void logInfo(Object msg) {
        this.log.info(msg.toString());
    }

    @Override
    public void logInfo(Object msg, Throwable thrown) {
        this.log.info(msg.toString(), thrown);
    }

    @Override
    public void logTrace(Object msg) {
        this.log.trace(msg.toString());
    }

    @Override
    public void logTrace(Object msg, Throwable thrown) {
        this.log.trace(msg.toString(), thrown);
    }

    @Override
    public void logWarn(Object msg) {
        this.log.warn(msg.toString());
    }

    @Override
    public void logWarn(Object msg, Throwable thrown) {
        this.log.warn(msg.toString(), thrown);
    }

}
