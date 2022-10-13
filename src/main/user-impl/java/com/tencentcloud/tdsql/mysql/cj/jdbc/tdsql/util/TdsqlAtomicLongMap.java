package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public final class TdsqlAtomicLongMap<K> implements Serializable {

    private final ConcurrentHashMap<K, NodeMsg> map;

    private TdsqlAtomicLongMap(ConcurrentHashMap<K, NodeMsg> map) {
        this.map = checkNotNull(map);
    }


    public static <K> TdsqlAtomicLongMap<K> create() {
        return new TdsqlAtomicLongMap<K>(new ConcurrentHashMap<>());
    }

    public static <K> TdsqlAtomicLongMap<K> create(Map<? extends K, ? extends NodeMsg> m) {
        TdsqlAtomicLongMap<K> result = create();
        result.putAll(m);
        return result;
    }

    public NodeMsg get(K key) {
        return map.getOrDefault(key, null);
    }

    public NodeMsg incrementAndGet(K key) {
        return addAndGet(key, 1);
    }

    public NodeMsg decrementAndGet(K key) {
        return addAndGet(key, -1);
    }

    public NodeMsg getAndIncrement(K key) {
        return getAndAdd(key, 1);
    }

    public NodeMsg getAndDecrement(K key) {
        return getAndAdd(key, -1);
    }

    public NodeMsg addAndGet(K key, long delta) {
        return accumulateAndGet(key, delta, Long::sum);
    }

    public NodeMsg getAndAdd(K key, long delta) {
        return getAndAccumulate(key, delta, Long::sum);
    }

    public NodeMsg accumulateAndGet(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return updateAndGet(key, null, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    public NodeMsg getAndAccumulate(K key, long x, LongBinaryOperator accumulatorFunction) {
        checkNotNull(accumulatorFunction);
        return getAndUpdate(key, null, oldValue -> accumulatorFunction.applyAsLong(oldValue, x));
    }

    /**
     * 先对map中的内容进行更新，然后返回更新之后的nodeMsg
     *
     * @param key
     * @param nodeMsg
     * @param updaterFunction
     * @return
     */
    public NodeMsg updateAndGet(K key, NodeMsg nodeMsg, LongUnaryOperator updaterFunction) {
        checkNotNull(updaterFunction);
        return map.compute(key, (k, value) -> {
            Long newValue = updaterFunction.applyAsLong((value == null) ? 0L : value.getCount().longValue());
            //如果map中key对应的值为空，那么就直接将传入的nodeMsg更新进去，不然就将newCount更新，其他的不变
            if (value == null) {
                return nodeMsg;
            } else {
                value.setCount(newValue);
                return value;
            }
        });
    }

    /**
     * 更新nodeMsg中的count的同时，返回旧值
     * 更新逻辑：如果map中有对应的key value，那么直接将value中的count值进行更新，如果没有对应的key value，那么就将value直接存进去即可
     *
     * @param key
     * @param nodeMsg
     * @param updaterFunction
     * @return
     */
    public NodeMsg getAndUpdate(K key, NodeMsg nodeMsg, LongUnaryOperator updaterFunction) {
        checkNotNull(updaterFunction);
        AtomicLong holder = new AtomicLong();
        NodeMsg temNodeMsg = map.getOrDefault(key, null);
        NodeMsg oldNodeMsg = null;
        if (temNodeMsg != null) {
            try {
                oldNodeMsg = (NodeMsg) temNodeMsg.clone();
            } catch (CloneNotSupportedException e) {
                String errMessage = "Object NodeMsg copy failed!";
                logError(errMessage, e);
            }
        }

        map.compute(
                key,
                (k, value) -> {
                    long oldValue = (value == null) ? 0L : value.getCount().longValue();
                    holder.set(oldValue);
                    long newValue = updaterFunction.applyAsLong(oldValue);
                    //如果key对应的value为空，那么就将传入的nodeMsg更新进去，不然就只更新count值
                    if (value == null) {
                        value = nodeMsg;
                    } else {
                        value.setCount(newValue);
                    }
                    return value;
                });
        if (oldNodeMsg != null) {
            oldNodeMsg.setCount(holder.get());
        }
        return oldNodeMsg;
    }

    public NodeMsg put(K key, NodeMsg newValue) {
        return getAndUpdate(key, newValue, x -> newValue.getCount());
    }

    public void putAll(Map<? extends K, ? extends NodeMsg> m) {
        m.forEach(this::put);
    }

    public NodeMsg remove(K key) {
        NodeMsg result = map.remove(key);
        return (result == null) ? null : result;
    }

    boolean remove(K key, NodeMsg value) {
        return map.remove(key, value);
    }

    public boolean removeIfZero(K key) {
        NodeMsg nodeMsg = map.get(key);
        return nodeMsg.getCount() == 0 ? remove(key, nodeMsg) : false;
    }

    public void removeAllZeros() {
        map.values().removeIf(x -> x.getCount() == 0);
    }

    public long sum() {
        return map.values().stream().collect(Collectors.summarizingLong(x -> x.getCount())).getSum();
    }

    private transient Map<K, NodeMsg> asMap;

    public Map<K, NodeMsg> asMap() {
        Map<K, NodeMsg> result = asMap;
        return (result == null) ? asMap = createAsMap() : result;
    }

    private Map<K, NodeMsg> createAsMap() {
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

    //还要修改
    NodeMsg putIfAbsent(K key, NodeMsg newValue) {
        AtomicBoolean noValue = new AtomicBoolean(false);
        NodeMsg result =
                map.compute(
                        key,
                        (k, oldValue) -> {
                            if (oldValue == null || oldValue.getCount() == 0) {
                                noValue.set(true);
                                return newValue;
                            } else {
                                return oldValue;
                            }
                        });
        return noValue.get() ? null : result;
    }

    boolean replace(K key, NodeMsg expectedOldValue, NodeMsg newValue) {
        if (expectedOldValue == null) {
            return putIfAbsent(key, newValue) == null;
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
