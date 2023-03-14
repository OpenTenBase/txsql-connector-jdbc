package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectTopoServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster.TdsqlDataSetCache;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.Objects;

/**
 * <p>
 * 每个DataSource中所共享的信息
 * </p>
 *
 * @author gyokumeixie@tencent.com
 */
public class TdsqlDirectInfo {

    private String datasourceUuid;
    private TdsqlDirectTopoServer topoServer;
    private TdsqlDataSetCache tdsqlDataSetCache;
    private TdsqlDirectConnectionManager tdsqlDirectConnectionManager;


    /**
     * <p>
     * 根据url中的信息，生成该数据源唯一的uuid
     * uuid由proxy ip:port +  database的形式表示以区分不同的数据源
     * </p>
     *
     * @param
     */
    public void setDatasourceUuid(ConnectionUrl connectionUrl) {
        this.datasourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(connectionUrl);
    }

    public String getDatasourceUuid() {
        return datasourceUuid;
    }

    /**
     * <p>
     * 初始化缓存
     * </p>
     *
     * @param datasourceUuid
     */
    public void initDataSetCache(String datasourceUuid) {
        this.tdsqlDataSetCache = new TdsqlDataSetCache(datasourceUuid);
    }

    /**
     * <p>
     * 初始化topoServer
     * </p>
     *
     * @param datasourceUuid
     */
    public void initTopoServer(String datasourceUuid) {
        this.topoServer = new TdsqlDirectTopoServer(datasourceUuid);
    }

    /**
     * <p>
     * 初始化TdsqlDirectConnectionManager
     * </p>
     *
     * @param datasourceUuid
     */
    public void initTdsqlDirectConnectionManager(String datasourceUuid) {
        this.tdsqlDirectConnectionManager = new TdsqlDirectConnectionManager(datasourceUuid);
    }

    public TdsqlDirectConnectionManager getTdsqlDirectConnectionManager() {
        return tdsqlDirectConnectionManager;
    }

    public TdsqlDirectTopoServer getTopoServer() {
        return topoServer;
    }

    public TdsqlDataSetCache getDataSetCache() {
        return tdsqlDataSetCache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectInfo that = (TdsqlDirectInfo) o;
        return datasourceUuid.equals(that.datasourceUuid)
                && topoServer.equals(that.topoServer)
                && tdsqlDataSetCache.equals(that.tdsqlDataSetCache)
                && tdsqlDirectConnectionManager.equals(that.tdsqlDirectConnectionManager);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, topoServer, tdsqlDataSetCache, tdsqlDirectConnectionManager);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
