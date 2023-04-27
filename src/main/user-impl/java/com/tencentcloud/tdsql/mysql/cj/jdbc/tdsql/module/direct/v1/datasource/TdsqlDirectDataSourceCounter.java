package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.datasource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *<p>
 *     直连模式数据源信息记录类
 *</p>
 *
 * @author gyokumeixie@tencent.com
 */
public class TdsqlDirectDataSourceCounter {
    private final Map<String, TdsqlDirectInfo> counterDatasourceMap = new HashMap<>();
    private final ReentrantReadWriteLock counterLock = new ReentrantReadWriteLock();

    /**
     * <p>
     * 初始化方法，当发现新的DataSourceUuid时触发
     * </p>
     *
     * @param tdsqlDirectInfo {@link TdsqlDirectInfo} 直连信息记录类对象
     */
    public void initialize(TdsqlDirectInfo tdsqlDirectInfo) {
        String datasourceUuid = tdsqlDirectInfo.getDatasourceUuid();
        if (!this.counterDatasourceMap.containsKey(datasourceUuid)) {
            try {
                tdsqlDirectInfo.initDataSetCache(datasourceUuid);
                tdsqlDirectInfo.initTopoServer(datasourceUuid);
                tdsqlDirectInfo.initTdsqlDirectConnectionManager(datasourceUuid);
                this.counterDatasourceMap.put(datasourceUuid, tdsqlDirectInfo);
            }finally {
            }

        }
    }

    public TdsqlDirectInfo getTdsqlDirectInfo(String dataSourceUuid){
        try {
            counterLock.readLock().lock();
            return this.counterDatasourceMap.get(dataSourceUuid);
        }finally {
            counterLock.readLock().unlock();
        }

    }

    public static TdsqlDirectDataSourceCounter getInstance() {
        return SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {
        private static final TdsqlDirectDataSourceCounter INSTANCE = new TdsqlDirectDataSourceCounter();
    }
}
