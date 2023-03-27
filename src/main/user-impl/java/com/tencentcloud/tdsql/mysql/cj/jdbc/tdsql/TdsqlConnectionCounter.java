package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import java.util.concurrent.atomic.LongAdder;

/**
 * <p>TDSQL专属，连接计数器接口</p>
 *
 * @author dorianzhang@tencent.com
 */
public interface TdsqlConnectionCounter {

    /**
     * 获取主机信息
     *
     * @return 主机信息
     */
    AbstractTdsqlHostInfo getTdsqlHostInfo();

    /**
     * 获取计数器
     *
     * @return 计数器
     */
    LongAdder getCount();
}
