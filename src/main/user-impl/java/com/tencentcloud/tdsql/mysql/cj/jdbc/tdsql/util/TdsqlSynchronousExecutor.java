package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import java.util.concurrent.Executor;

/**
 * <p>
 * Special executor used only to work around a MySQL issue that has not been addressed.
 * MySQL issue: <a href="http://bugs.mysql.com/bug.php?id=75615">...</a>
 * </p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlSynchronousExecutor implements Executor {

    private final String datasourceUuid;

    public TdsqlSynchronousExecutor(String datasourceUuid) {
        this.datasourceUuid = datasourceUuid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        try {
            command.run();
        } catch (Exception t) {
            TdsqlLoggerFactory.logError(this.datasourceUuid, "Failed to execute: " + command, t);
        }
    }
}
