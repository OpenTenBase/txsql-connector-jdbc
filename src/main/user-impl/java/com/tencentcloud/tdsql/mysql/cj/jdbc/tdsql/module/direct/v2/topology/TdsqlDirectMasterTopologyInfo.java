package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.AT_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.COLON_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EMPTY_STRING;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectParseTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.util.List;
import java.util.Objects;

/**
 * TDSQL专属，直连模式主机拓扑信息类，格式：IP:PORT@权重@是否存活
 * <ul>
 *     <li>权重取值范围，[0-100]</li>
 *     <li>是否存活取值，0：正常，-1：不存活</li>
 * </ul>
 */
public class TdsqlDirectMasterTopologyInfo {

    private static final TdsqlDirectMasterTopologyInfo EMPTY_MASTER = new TdsqlDirectMasterTopologyInfo();
    private final String datasourceUuid;
    private final String ip;
    private final Integer port;
    private final Integer weight;
    private final Integer isAlive;
    private String originalInfoStr;

    /**
     * 创建空的主库拓扑信息
     *
     * @return {@link TdsqlDirectMasterTopologyInfo#EMPTY_MASTER}
     */
    public static TdsqlDirectMasterTopologyInfo emptyMaster() {
        return EMPTY_MASTER;
    }

    /**
     * 判断是否空的主库拓扑信息
     *
     * @return 如果是空的返回 {@code true}，否则返回 {@code false}
     */
    public boolean isEmptyMaster() {
        return EMPTY_STRING.equals(this.ip) && this.port == -99 && this.weight == -99 && this.isAlive == -99;
    }

    /**
     * 空的主库拓扑信息构造方法
     */
    private TdsqlDirectMasterTopologyInfo() {
        this.datasourceUuid = EMPTY_STRING;
        this.ip = EMPTY_STRING;
        this.port = -99;
        this.weight = -99;
        this.isAlive = -99;
    }

    /**
     * 根据主库拓扑信息字符串，创建主库拓扑信息
     *
     * @param datasourceUuid 数据源UUID
     * @param originalInfoStr 主库拓扑信息字符串
     */
    public TdsqlDirectMasterTopologyInfo(String datasourceUuid, String originalInfoStr) {
        List<String> tempList = StringUtils.split(originalInfoStr, AT_MARK, /*trim*/ true);
        if (tempList == null || tempList.size() != 3) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidMasterInfoFormat",
                            new Object[]{originalInfoStr}));
        }

        String ipPortStr = tempList.get(0);
        if (StringUtils.isEmptyOrWhitespaceOnly(ipPortStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterIpAndPort"));
        }

        List<String> ipPortTempList = StringUtils.split(ipPortStr, COLON_MARK, /*trim*/ true);
        if (ipPortTempList == null || ipPortTempList.size() != 2) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidMasterIpAndPortFormat",
                            new Object[]{ipPortStr}));
        }
        String ipStr = ipPortTempList.get(0);
        if (StringUtils.isEmptyOrWhitespaceOnly(ipStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterIp"));
        }
        this.ip = ipStr;

        String portStr = ipPortTempList.get(1);
        if (StringUtils.isEmptyOrWhitespaceOnly(portStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterPort"));
        }
        try {
            this.port = Integer.parseInt(portStr);
            if (this.port <= 0 || this.port > 65535) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterPort",
                                new Object[]{portStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterPort",
                            new Object[]{portStr}));
        }

        String weightStr = tempList.get(1);
        if (StringUtils.isEmptyOrWhitespaceOnly(weightStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterWeight"));
        }
        try {
            this.weight = Integer.parseInt(weightStr);
            if (this.weight < 0 || this.weight > 100) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterWeight",
                                new Object[]{weightStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterWeight",
                            new Object[]{weightStr}));
        }

        String isAliveStr = tempList.get(2);
        if (StringUtils.isEmptyOrWhitespaceOnly(isAliveStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterAlive"));
        }
        try {
            this.isAlive = Integer.parseInt(isAliveStr);
            if (this.isAlive != 0 && this.isAlive != -1) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterAlive",
                                new Object[]{isAliveStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfMasterAlive",
                            new Object[]{isAliveStr}));
        }

        this.datasourceUuid = datasourceUuid;
        this.originalInfoStr = originalInfoStr;
    }

    /**
     * 判断主库拓扑信息是否有效
     *
     * @return 有效返回 {@code true}，否则返回 {@code false}
     */
    public boolean isValid() {
        if (this.isEmptyMaster()) {
            TdsqlLoggerFactory.logError(this.datasourceUuid,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidMasterInfoValue",
                            new Object[]{this}));
            return false;
        }
        if (StringUtils.isEmptyOrWhitespaceOnly(this.getIp())) {
            TdsqlLoggerFactory.logError(this.datasourceUuid,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidMasterInfoValue",
                            new Object[]{this}));
            return false;
        }
        Integer port = this.getPort();
        if (port == null || port <= 0 || port > 65535) {
            TdsqlLoggerFactory.logError(this.datasourceUuid,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidMasterInfoValue",
                            new Object[]{this}));
            return false;
        }
        return true;
    }

    /**
     * 将拓扑信息转换为直连模式专属主机信息
     *
     * @param directDataSourceConfig 数据源配置
     * @return 直连模式专属主机信息
     */
    public TdsqlDirectHostInfo convertToDirectHostInfo(TdsqlDirectDataSourceConfig directDataSourceConfig) {
        HostInfo mainHost = directDataSourceConfig.getConnectionUrl().getMainHost();
        DatabaseUrlContainer originalUrl = mainHost.getOriginalUrl();
        return new TdsqlDirectHostInfo(directDataSourceConfig,
                new HostInfo(originalUrl, this.ip, this.port, mainHost.getUser(), mainHost.getPassword(),
                        mainHost.getHostProperties()), this.weight, this.isAlive);
    }

    public String printPretty() {
        return String.format("[Master:%s]", this.getHostPortPair());
    }

    public String getIp() {
        return ip;
    }

    public Integer getPort() {
        return port;
    }

    public Integer getWeight() {
        return weight;
    }

    public Integer getIsAlive() {
        return isAlive;
    }

    public String getOriginalInfoStr() {
        return originalInfoStr;
    }

    public String getHostPortPair() {
        return this.ip + COLON_MARK + this.port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectMasterTopologyInfo that = (TdsqlDirectMasterTopologyInfo) o;
        return Objects.equals(datasourceUuid, that.datasourceUuid) && Objects.equals(ip, that.ip)
                && Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, ip, port);
    }

    @Override
    public String toString() {
        return this.originalInfoStr;
    }
}
