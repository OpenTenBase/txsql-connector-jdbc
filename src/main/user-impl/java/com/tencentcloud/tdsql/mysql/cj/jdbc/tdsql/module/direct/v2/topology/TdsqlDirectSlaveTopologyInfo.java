package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.AT_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.COLON_MARK;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectParseTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule.TdsqlDirectHostInfo;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.util.List;
import java.util.Objects;

/**
 * <p>
 * 备机拓扑信息类，格式：IP:PORT@权重@是否watch节点@主备延迟
 * <ul>
 *     <li>权重取值范围，[0-100]</li>
 *     <li>是否watch节点，0：正常节点，1：watch节点</li>
 *     <li>主备延迟取值范围，大于等于0</li>
 * </ul>
 * </p>
 */
public class TdsqlDirectSlaveTopologyInfo {

    private final String datasourceUuid;
    private final String ip;
    private final Integer port;
    private final Integer weight;
    private final Integer isWatch;
    private final Integer delay;
    private String slaveInfoStr;

    public TdsqlDirectSlaveTopologyInfo(String datasourceUuid, String ip, Integer port, Integer weight, Integer isWatch,
            Integer delay) {
        this.datasourceUuid = datasourceUuid;
        this.ip = ip;
        this.port = port;
        this.weight = weight;
        this.isWatch = isWatch;
        this.delay = delay;
    }

    public TdsqlDirectSlaveTopologyInfo(String datasourceUuid, String slaveInfoStr) {
        List<String> tempList = StringUtils.split(slaveInfoStr, AT_MARK, /*trim*/ true);
        if (tempList == null || tempList.size() != 4) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidSlavesInfoFormat",
                            new Object[]{slaveInfoStr}));
        }

        String ipPortStr = tempList.get(0);
        if (StringUtils.isEmptyOrWhitespaceOnly(ipPortStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlaveIpAndPort"));
        }

        List<String> ipPortTempList = StringUtils.split(ipPortStr, COLON_MARK, /*trim*/ true);
        if (ipPortTempList == null || ipPortTempList.size() != 2) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidSlaveIpAndPortFormat",
                            new Object[]{ipPortStr}));
        }
        String ipStr = ipPortTempList.get(0);
        if (StringUtils.isEmptyOrWhitespaceOnly(ipStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlaveIp"));
        }
        this.ip = ipStr;

        String portStr = ipPortTempList.get(1);
        if (StringUtils.isEmptyOrWhitespaceOnly(portStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlavePort"));
        }
        try {
            this.port = Integer.parseInt(portStr);
            if (this.port <= 0 || this.port > 65535) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlavePort",
                                new Object[]{portStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlavePort",
                            new Object[]{portStr}));
        }

        String weightStr = tempList.get(1);
        if (StringUtils.isEmptyOrWhitespaceOnly(weightStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlaveWeight"));
        }
        try {
            this.weight = Integer.parseInt(weightStr);
            if (this.weight < 0 || this.weight > 100) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlavesWeight",
                                new Object[]{weightStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlavesWeight",
                            new Object[]{weightStr}));
        }

        String isWatchStr = tempList.get(2);
        if (StringUtils.isEmptyOrWhitespaceOnly(isWatchStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlaveIsWatch"));
        }
        try {
            this.isWatch = Integer.parseInt(isWatchStr);
            if (this.isWatch != 0 && this.isWatch != 1) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlaveIsWatch",
                                new Object[]{isWatchStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlaveIsWatch",
                            new Object[]{isWatchStr}));
        }

        String delayStr = tempList.get(3);
        if (StringUtils.isEmptyOrWhitespaceOnly(delayStr)) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlaveDelay"));
        }
        try {
            this.delay = Integer.parseInt(delayStr);
            if (this.delay < 0) {
                throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlaveDelay",
                                new Object[]{delayStr}));
            }
        } catch (NumberFormatException e) {
            throw TdsqlExceptionFactory.logException(datasourceUuid, TdsqlDirectParseTopologyException.class,
                    Messages.getString("TdsqlDirectParseTopologyException.InvalidIntValueOfSlaveDelay",
                            new Object[]{delayStr}));
        }

        this.datasourceUuid = datasourceUuid;
        this.slaveInfoStr = slaveInfoStr;
    }

    public TdsqlDirectHostInfo convertToDirectHostInfo(TdsqlDirectDataSourceConfig directDataSourceConfig) {
        HostInfo mainHost = directDataSourceConfig.getConnectionUrl().getMainHost();
        DatabaseUrlContainer originalUrl = mainHost.getOriginalUrl();
        return new TdsqlDirectHostInfo(directDataSourceConfig,
                new HostInfo(originalUrl, this.ip, this.port, mainHost.getUser(), mainHost.getPassword(),
                        mainHost.getHostProperties()), this.weight, this.isWatch, this.delay);
    }

    public String printPretty() {
        return String.format("[Slave:%s,weight:%d,delay:%d]", this.getHostPortPair(), this.getWeight(),
                this.getDelay());
    }

    public String getDatasourceUuid() {
        return datasourceUuid;
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

    public Integer getIsWatch() {
        return isWatch;
    }

    public Integer getDelay() {
        return delay;
    }

    public String getHostPortPair() {
        return this.ip + COLON_MARK + this.port;
    }

    public String getSlaveInfoStr() {
        return slaveInfoStr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDirectSlaveTopologyInfo that = (TdsqlDirectSlaveTopologyInfo) o;
        return Objects.equals(datasourceUuid, that.datasourceUuid) && Objects.equals(ip, that.ip)
                && Objects.equals(port, that.port) && Objects.equals(weight, that.weight)
                && Objects.equals(delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceUuid, ip, port, weight, delay);
    }

    @Override
    public String toString() {
        return this.slaveInfoStr;
    }
}
