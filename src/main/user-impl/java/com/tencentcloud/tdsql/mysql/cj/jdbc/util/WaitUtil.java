package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class WaitUtil {
    public static boolean waitFor(int interval, int count, Supplier<Boolean> condition) throws InterruptedException {
        int curCount = 0;
        while (true) {
            if(condition.get()) {
                return true;
            }
            curCount += 1;
            TimeUnit.SECONDS.sleep(interval);
            if(curCount > count) {
                return false;
            }
        }
    }

}
