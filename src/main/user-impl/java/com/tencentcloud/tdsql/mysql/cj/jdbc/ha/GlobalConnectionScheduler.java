package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.jdbc.util.ActiveConnectionCounter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author dorianzhang@tencent.com
 */
public class GlobalConnectionScheduler {

    private volatile static GlobalConnectionScheduler instance = null;
    private final ActiveConnectionCounter<String> counter;
    private final ReentrantLock lock = new ReentrantLock(true);

    private GlobalConnectionScheduler() {
        this.counter = ActiveConnectionCounter.create(new ConcurrentHashMap<>());
    }

    private GlobalConnectionScheduler(Map<String, Long> m) {
        this.counter = ActiveConnectionCounter.create(m);
    }

    public static GlobalConnectionScheduler getInstance() {
        if (instance == null) {
            synchronized (GlobalConnectionScheduler.class) {
                if (instance == null) {
                    instance = new GlobalConnectionScheduler();
                }
            }
        }
        return instance;
    }

    public static GlobalConnectionScheduler getInstance(Map<String, Long> m) {
        if (instance == null) {
            synchronized (GlobalConnectionScheduler.class) {
                if (instance == null) {
                    instance = new GlobalConnectionScheduler(m);
                }
            }
        }
        return instance;
    }

    public ActiveConnectionCounter<String> getCounter() {
        return this.counter;
    }

    public Lock getLock() {
        return this.lock;
    }
}
