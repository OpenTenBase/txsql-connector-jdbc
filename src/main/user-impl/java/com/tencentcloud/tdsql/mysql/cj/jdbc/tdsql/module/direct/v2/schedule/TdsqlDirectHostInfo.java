package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.AbstractTdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import java.util.HashMap;
import java.util.Objects;

/**
 * <p>TDSQL专属，直连模式主机信息类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectHostInfo extends AbstractTdsqlHostInfo {

    private static final TdsqlDirectHostInfo EMPTY = new TdsqlDirectHostInfo();
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final Integer isAlive;
    private final Integer isWatch;
    private final Integer delay;

    /**
     * 主库构造方法
     *
     * @param dataSourceConfig 数据源配置
     * @param hostInfo {@link HostInfo}
     * @param weight 权重
     * @param isAlive 是否存活
     */
    public TdsqlDirectHostInfo(TdsqlDirectDataSourceConfig dataSourceConfig, HostInfo hostInfo, Integer weight,
            Integer isAlive) {
        this(dataSourceConfig, hostInfo, weight, isAlive, -99, -99);
    }

    /**
     * 备库构造方法
     *
     * @param dataSourceConfig 数据源配置
     * @param hostInfo {@link HostInfo}
     * @param weight 权重
     * @param isWatch 是否Watch节点
     * @param delay 延迟
     */
    public TdsqlDirectHostInfo(TdsqlDirectDataSourceConfig dataSourceConfig, HostInfo hostInfo, Integer weight,
            Integer isWatch, Integer delay) {
        this(dataSourceConfig, hostInfo, weight, -99, isWatch, delay);
    }

    /**
     * 空主机信息的构造方法
     */
    private TdsqlDirectHostInfo() {
        super();
        this.dataSourceUuid = TdsqlDataSourceUuidGenerator.UNKNOWN_UUID;
        this.dataSourceConfig = null;
        this.connectionMode = TdsqlConnectionModeEnum.DIRECT;
        this.originalUrl = null;
        this.host = null;
        this.port = NO_PORT;
        this.user = null;
        this.password = null;
        this.hostProperties = new HashMap<>();
        this.weight = -99;
        this.isAlive = -99;
        this.isWatch = -99;
        this.delay = -99;
    }

    /**
     * 构造方法
     *
     * @param dataSourceConfig 数据源配置信息
     * @param hostInfo 主机信息
     * @param weight 权重
     * @param isAlive 是否存活
     * @param isWatch 是否监视节点
     * @param delay 延迟信息
     */
    private TdsqlDirectHostInfo(TdsqlDirectDataSourceConfig dataSourceConfig, HostInfo hostInfo, Integer weight,
            Integer isAlive, Integer isWatch, Integer delay) {
        super(hostInfo);
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
        this.connectionMode = TdsqlConnectionModeEnum.DIRECT;
        this.originalUrl = hostInfo.getOriginalUrl();
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
        this.user = hostInfo.getUser();
        this.password = hostInfo.getPassword();
        this.hostProperties = hostInfo.getHostProperties();
        this.weight = weight;
        this.isAlive = isAlive;
        this.isWatch = isWatch;
        this.delay = delay;
    }

    /**
     * 创建空的主机信息
     *
     * @return {@link TdsqlDirectHostInfo#EMPTY}
     */
    public static TdsqlDirectHostInfo empty() {
        return EMPTY;
    }

    /**
     * 判断是否是空的主机信息
     *
     * @return 如果是则返回 {@code true}，否则返回 {@code false}
     */
    public boolean isEmptyDirectHostInfo() {
        return this.equals(EMPTY);
    }

    public String getDataSourceUuid() {
        return dataSourceUuid;
    }

    public TdsqlDirectDataSourceConfig getDataSourceConfig() {
        return dataSourceConfig;
    }

    public TdsqlConnectionModeEnum getConnectionMode() {
        return connectionMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectHostInfo that = (TdsqlDirectHostInfo) o;
        return port == that.port && Objects.equals(dataSourceUuid, that.dataSourceUuid)
                && connectionMode == that.connectionMode && Objects.equals(originalUrl, that.originalUrl)
                && Objects.equals(host, that.host) && Objects.equals(user, that.user) && Objects.equals(password,
                that.password) && Objects.equals(hostProperties, that.hostProperties) && Objects.equals(weight,
                that.weight) && Objects.equals(isAlive, that.isAlive) && Objects.equals(isWatch, that.isWatch)
                && Objects.equals(delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSourceUuid, connectionMode, originalUrl, host, port, user, password, hostProperties,
                weight, isAlive, isWatch, delay);
    }

    @Override
    public String toString() {
        return "TdsqlDirectHostInfo{" +
                "dataSourceUuid='" + dataSourceUuid + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", hostProperties=" + hostProperties +
                ", weight=" + weight +
                ", isAlive=" + isAlive +
                ", isWatch=" + isWatch +
                ", delay=" + delay +
                '}';
    }
}
