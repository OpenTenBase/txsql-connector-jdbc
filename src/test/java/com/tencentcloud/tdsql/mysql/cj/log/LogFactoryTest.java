package com.tencentcloud.tdsql.mysql.cj.log;

import org.junit.jupiter.api.Test;

class LogFactoryTest {
    @Test
    public void Slf4JTest(){
        Log logger = LogFactory.getLogger("Slf4JLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void StandardLoggerTest(){
        Log logger = LogFactory.getLogger("StandardLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void JdkLoggerTest(){
        Log logger = LogFactory.getLogger("JdkLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void LogbackLoggerTest(){
        Log logger = LogFactory.getLogger("LogbackLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void Log4JLoggerTest(){
        Log logger = LogFactory.getLogger("Log4JLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void CommonsLoggerTest(){
        Log logger = LogFactory.getLogger("CommonsLogger", "Seimin");
        printLog(logger);
    }

    @Test
    public void Log4J2LoggerTest(){
        Log logger = LogFactory.getLogger("Log4J2Logger", "Seimin");
        printLog(logger);
    }




    public void printLog(Log logger){
        System.out.println(logger.getClass());
        logger.logInfo("info");
        logger.logDebug("debug");
        logger.logError("error");
        logger.logFatal("fatal");
        logger.logTrace("trace");
        logger.logWarn("warn");
    }


}