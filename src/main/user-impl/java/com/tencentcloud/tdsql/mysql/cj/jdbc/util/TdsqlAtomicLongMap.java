package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlAtomicLongMap<K> implements Serializable {
    private final ConcurrentHashMap<K, Long> map;

    private TdsqlAtomicLongMap(ConcurrentHashMap<K, Long> map) {
        this.map = checkNotNull(map);
    }

    public static <K> TdsqlAtomicLongMap<K> create() {
        return new TdsqlAtomicLongMap<K>(new ConcurrentHashMap<>());
    }

    public static <K> TdsqlAtomicLongMap<K> create(Map<? extends K, ? extends Long> m) {
        TdsqlAtomicLongMap<K> result = create();
        result.putAll(m);
        return result;
    }

    public long get(K key) {
        return map.getOrDefault(key, 0L);
    }

    public long incrementAndGet(K key) {
        return addAndGet(key, 1);
    }

    public long decrementAndGet(K key) {
        return addAndGet(key, -1);
    }

    public long addAndGet(K key, long delta) {
        return accumulateAndGet(key, delta, Long::sum);
    }

    public long getAndIncrement(K key) {
        return getAndAdd(key, 1);
    }

    public long getAndDecrement(K key) {
        return getAndAdd(key, -1);
    }

    public long getAndAdd(K key, long delta) {
        return getAndAccumulate(key, delta, Long::sum);
    }

    public long updateAndGet(K key, LongUnaryOperator updaterFunction) {
        checkNotNull(updaterFunction);
        return map.compute(key, (k, value) -> updaterFunction.applyAsLong((value == null) ? 0L : value.longValue()));
    }

    public long getAndUpdate(K key, LongUnaryOperator updaterFunction) {
        checkNotNull(updaterFunction);
        AtomicLong holder = new AtomicLong();
        map.compute(
                key,
                (k, value) -> {
                    long oldValue = (value == null) ? 0L : value.longValue();
                    holder.set(oldValue);
                    return updaterFunction.applyAsLong(oldValue);
                });
        return holder.get();
    }

    public long accumulateAndGet(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return updateAndGet(key, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    public long getAndAccumulate(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return getAndUpdate(key, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    public long put(K key, long newValue) {
        return getAndUpdate(key, x -> newValue);
    }

    public void putAll(Map<? extends K, ? extends Long> m) {
        m.forEach(this::put);
    }

    public long remove(K key) {
        Long result = map.remove(key);
        return (result == null) ? 0L : result.longValue();
    }

    boolean remove(K key, long value) {
        return map.remove(key, value);
    }

    public boolean removeIfZero(K key) {
        return remove(key, 0);
    }

    public void removeAllZeros() {
        map.values().removeIf(x -> x == 0);
    }

    public long sum() {
        return map.values().stream().mapToLong(Long::longValue).sum();
    }

    private transient Map<K, Long> asMap;

    public Map<K, Long> asMap() {
        Map<K, Long> result = asMap;
        return (result == null) ? asMap = createAsMap() : result;
    }

    private Map<K, Long> createAsMap() {
        return Collections.unmodifiableMap(map);
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public void clear() {
        map.clear();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    long putIfAbsent(K key, long newValue) {
        AtomicBoolean noValue = new AtomicBoolean(false);
        Long result =
                map.compute(
                        key,
                        (k, oldValue) -> {
                            if (oldValue == null || oldValue == 0) {
                                noValue.set(true);
                                return newValue;
                            } else {
                                return oldValue;
                            }
                        });
        return noValue.get() ? 0L : result.longValue();
    }

    boolean replace(K key, long expectedOldValue, long newValue) {
        if (expectedOldValue == 0L) {
            return putIfAbsent(key, newValue) == 0L;
        } else {
            return map.replace(key, expectedOldValue, newValue);
        }
    }

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }
}
