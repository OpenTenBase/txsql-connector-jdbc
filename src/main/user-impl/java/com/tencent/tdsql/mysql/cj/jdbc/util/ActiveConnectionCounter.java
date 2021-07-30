package com.tencent.tdsql.mysql.cj.jdbc.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * @author dorianzhang@tencent.com
 */
public class ActiveConnectionCounter<K> implements Serializable {

    private final ConcurrentHashMap<K, Long> map;

    private ActiveConnectionCounter(ConcurrentHashMap<K, Long> map) {
        this.map = checkNotNull(map);
    }

    /**
     * Creates an {@code ActiveConnectionCounter}.
     */
    public static <K> ActiveConnectionCounter<K> create() {
        return new ActiveConnectionCounter<K>(new ConcurrentHashMap<>());
    }

    /**
     * Creates an {@code ActiveConnectionCounter} with the same mappings as the specified {@code Map}.
     */
    public static <K> ActiveConnectionCounter<K> create(Map<? extends K, ? extends Long> m) {
        ActiveConnectionCounter<K> result = create();
        result.putAll(m);
        return result;
    }

    /**
     * Returns the value associated with {@code key}, or zero if there is no value associated with
     * {@code key}.
     */
    public long get(K key) {
        return map.getOrDefault(key, 0L);
    }

    /**
     * Increments by one the value currently associated with {@code key}, and returns the new value.
     */
    public long incrementAndGet(K key) {
        return addAndGet(key, 1);
    }

    /**
     * Decrements by one the value currently associated with {@code key}, and returns the new value.
     */
    public long decrementAndGet(K key) {
        return addAndGet(key, -1);
    }

    /**
     * Adds {@code delta} to the value currently associated with {@code key}, and returns the new
     * value.
     */
    public long addAndGet(K key, long delta) {
        return accumulateAndGet(key, delta, Long::sum);
    }

    /**
     * Increments by one the value currently associated with {@code key}, and returns the old value.
     */
    public long getAndIncrement(K key) {
        return getAndAdd(key, 1);
    }

    /**
     * Decrements by one the value currently associated with {@code key}, and returns the old value.
     */
    public long getAndDecrement(K key) {
        return getAndAdd(key, -1);
    }

    /**
     * Adds {@code delta} to the value currently associated with {@code key}, and returns the old
     * value.
     */
    public long getAndAdd(K key, long delta) {
        return getAndAccumulate(key, delta, Long::sum);
    }

    /**
     * Updates the value currently associated with {@code key} with the specified function, and
     * returns the new value. If there is not currently a value associated with {@code key}, the
     * function is applied to {@code 0L}.
     */
    public long updateAndGet(K key, LongUnaryOperator updaterFunction) {
        checkNotNull(updaterFunction);
        return map.compute(
                key, (k, value) -> updaterFunction.applyAsLong((value == null) ? 0L : value.longValue()));
    }

    /**
     * Updates the value currently associated with {@code key} with the specified function, and
     * returns the old value. If there is not currently a value associated with {@code key}, the
     * function is applied to {@code 0L}.
     */
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

    /**
     * Updates the value currently associated with {@code key} by combining it with {@code x} via the
     * specified accumulator function, returning the new value. The previous value associated with
     * {@code key} (or zero, if there is none) is passed as the first argument to {@code
     * accumulatorFunction}, and {@code x} is passed as the second argument.
     */
    public long accumulateAndGet(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return updateAndGet(key, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    /**
     * Updates the value currently associated with {@code key} by combining it with {@code x} via the
     * specified accumulator function, returning the old value. The previous value associated with
     * {@code key} (or zero, if there is none) is passed as the first argument to {@code
     * accumulatorFunction}, and {@code x} is passed as the second argument.
     */
    public long getAndAccumulate(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return getAndUpdate(key, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    /**
     * Associates {@code newValue} with {@code key} in this map, and returns the value previously
     * associated with {@code key}, or zero if there was no such value.
     */
    public long put(K key, long newValue) {
        return getAndUpdate(key, x -> newValue);
    }

    /**
     * Copies all of the mappings from the specified map to this map. The effect of this call is
     * equivalent to that of calling {@code put(k, v)} on this map once for each mapping from key
     * {@code k} to value {@code v} in the specified map. The behavior of this operation is undefined
     * if the specified map is modified while the operation is in progress.
     */
    public void putAll(Map<? extends K, ? extends Long> m) {
        m.forEach(this::put);
    }

    /**
     * Removes and returns the value associated with {@code key}. If {@code key} is not in the map,
     * this method has no effect and returns zero.
     */
    public long remove(K key) {
        Long result = map.remove(key);
        return (result == null) ? 0L : result.longValue();
    }

    /**
     * If {@code (key, value)} is currently in the map, this method removes it and returns true;
     * otherwise, this method returns false.
     */
    boolean remove(K key, long value) {
        return map.remove(key, value);
    }

    /**
     * Atomically remove {@code key} from the map iff its associated value is 0.
     */
    public boolean removeIfZero(K key) {
        return remove(key, 0);
    }

    /**
     * Removes all mappings from this map whose values are zero.
     *
     * <p>This method is not atomic: the map may be visible in intermediate states, where some of the
     * zero values have been removed and others have not.
     */
    public void removeAllZeros() {
        map.values().removeIf(x -> x == 0);
    }

    /**
     * Returns the sum of all values in this map.
     *
     * <p>This method is not atomic: the sum may or may not include other concurrent operations.
     */
    public long sum() {
        return map.values().stream().mapToLong(Long::longValue).sum();
    }

    private transient Map<K, Long> asMap;

    /**
     * Returns a live, read-only view of the map backing this {@code ActiveConnectionCounter}.
     */
    public Map<K, Long> asMap() {
        Map<K, Long> result = asMap;
        return (result == null) ? asMap = createAsMap() : result;
    }

    private Map<K, Long> createAsMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns true if this map contains a mapping for the specified key.
     */
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /**
     * Returns the number of key-value mappings in this map. If the map contains more than {@code
     * Integer.MAX_VALUE} elements, returns {@code Integer.MAX_VALUE}.
     */
    public int size() {
        return map.size();
    }

    /**
     * Returns {@code true} if this map contains no key-value mappings.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Removes all of the mappings from this map. The map will be empty after this call returns.
     *
     * <p>This method is not atomic: the map may not be empty after returning if there were concurrent
     * writes.
     */
    public void clear() {
        map.clear();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    /**
     * If {@code key} is not already associated with a value or if {@code key} is associated with
     * zero, associate it with {@code newValue}. Returns the previous value associated with {@code
     * key}, or zero if there was no mapping for {@code key}.
     */
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
        return noValue.get() ? 0L : result;
    }

    /**
     * If {@code (key, expectedOldValue)} is currently in the map, this method replaces {@code
     * expectedOldValue} with {@code newValue} and returns true; otherwise, this method returns false.
     *
     * <p>If {@code expectedOldValue} is zero, this method will succeed if {@code (key, zero)} is
     * currently in the map, or if {@code key} is not in the map at all.
     */
    boolean replace(K key, long expectedOldValue, long newValue) {
        if (expectedOldValue == 0L) {
            return putIfAbsent(key, newValue) == 0L;
        } else {
            return map.replace(key, expectedOldValue, newValue);
        }
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }
}
