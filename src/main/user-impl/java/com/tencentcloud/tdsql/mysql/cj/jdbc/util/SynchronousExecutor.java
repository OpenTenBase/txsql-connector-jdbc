package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import java.util.concurrent.Executor;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class SynchronousExecutor implements Executor {

    @Override
    public void execute(Runnable command) {
        try {
            command.run();
        } catch (Exception t) {
            TdsqlDirectLoggerFactory.logError("Failed to execute: " + command, t);
        }
    }
}
