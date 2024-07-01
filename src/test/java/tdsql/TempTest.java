package tdsql;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Disabled
public class TempTest {
    @Test
    public void testNullEqual() {
        Integer id = null;

        if (Objects.equals(id, Integer.valueOf(1))) {
            System.out.println("true");
        }
        if (id < 0) {
            System.out.println("false");
        }
    }

    @Test
    public void testLock() {
        // 测试lock时unlock异常
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Lock readLock = lock.readLock();
        readLock.unlock();
    }
}
