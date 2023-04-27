package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.util.Map;
import java.util.Objects;

/**
 * <p>TDSQL专属，连接信息类</p>
 *
 * @author dorianzhang@tencent.com
 */
public final class TdsqlHostInfo extends HostInfo {

    private String ownerUuid;
    private final TdsqlConnectionModeEnum connectionMode;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    private final Map<String, String> hostProperties;
    private int weightFactor;
    private boolean alive;
    private Long delay;

    public TdsqlHostInfo(HostInfo hostInfo) {
        super(hostInfo.getOriginalUrl(), hostInfo.getHost(), hostInfo.getPort(), hostInfo.getUser(),
                hostInfo.getPassword(), hostInfo.getHostProperties());
        this.connectionMode = TdsqlConnectionModeEnum.UNKNOWN;
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
        this.user = hostInfo.getUser();
        this.password = hostInfo.getPassword();
        this.hostProperties = hostInfo.getHostProperties();
        this.database = hostInfo.getDatabase();
    }

    public TdsqlHostInfo(HostInfo hostInfo, TdsqlConnectionModeEnum connectionMode) {
        super(hostInfo.getOriginalUrl(), hostInfo.getHost(), hostInfo.getPort(), hostInfo.getUser(),
                hostInfo.getPassword(), hostInfo.getHostProperties());
        this.connectionMode = connectionMode;
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
        this.user = hostInfo.getUser();
        this.password = hostInfo.getPassword();
        this.hostProperties = hostInfo.getHostProperties();
        this.database = hostInfo.getDatabase();
    }

    public String getOwnerUuid() {
        return ownerUuid;
    }

    public void setOwnerUuid(String ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    public TdsqlConnectionModeEnum getConnectionMode() {
        return connectionMode;
    }

    public int getWeightFactor() {
        return weightFactor;
    }

    public void setWeightFactor(int weightFactor) {
        this.weightFactor = weightFactor;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public Long getDelay() {
        return this.delay;
    }

    public boolean getAlive() {
        return this.alive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlHostInfo that = (TdsqlHostInfo) o;
        return port == that.port && Objects.equals(host, that.host) && Objects.equals(user, that.user)
                && Objects.equals(password, that.password) && Objects.equals(database, that.database)
                && Objects.equals(hostProperties, that.hostProperties) && Objects.equals(connectionMode,
                that.connectionMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionMode, host, port, user, password, database, hostProperties);
    }
}
