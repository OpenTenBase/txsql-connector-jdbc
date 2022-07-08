package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectLoggerFactory;

/**
 * <p>
 * 自定义可以捕获异常的、实现了 {@link Runnable} 接口的抽象类
 * </p>
 *
 * @author dorianzhang@tencent.com
 */
public abstract class AbstractTdsqlCaughtRunnable implements Runnable {

    @Override
    public final void run() {
        try {
            caughtAndRun();
        } catch (Exception e) {
            exceptionHandler(e);
        }
    }

    /**
     * 捕获并运行
     */
    public abstract void caughtAndRun();

    private void exceptionHandler(Exception e) {
        TdsqlDirectLoggerFactory.logError(e.getMessage(), e);
    }
}
