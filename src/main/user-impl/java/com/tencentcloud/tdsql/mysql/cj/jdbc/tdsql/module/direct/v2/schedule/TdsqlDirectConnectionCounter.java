package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * <p>TDSQL专属，直连模式连接计数器类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectConnectionCounter implements TdsqlConnectionCounter {

    private final TdsqlDirectHostInfo directHostInfo;
    private final LongAdder count;

    public TdsqlDirectConnectionCounter(TdsqlDirectHostInfo directHostInfo) {
        this(directHostInfo, new LongAdder());
    }

    public TdsqlDirectConnectionCounter(TdsqlDirectHostInfo directHostInfo, LongAdder count) {
        this.directHostInfo = directHostInfo;
        this.count = count;
    }

    @Override
    public TdsqlDirectHostInfo getTdsqlHostInfo() {
        return directHostInfo;
    }

    @Override
    public LongAdder getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectConnectionCounter that = (TdsqlDirectConnectionCounter) o;
        return Objects.equals(directHostInfo, that.directHostInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directHostInfo);
    }

    @Override
    public String toString() {
        return "TdsqlDirectConnectionCounter{" +
                "directHostInfo=" + directHostInfo +
                ", count=" + count.longValue() +
                '}';
    }
}
