package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;

import java.util.HashMap;
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
    long heartbeatIntervalTime;

    public TdsqlHostInfo(HostInfo hostInfo) {
        super(hostInfo.getOriginalUrl(), hostInfo.getHost(), hostInfo.getPort(), hostInfo.getUser(),
                hostInfo.getPassword(), hostInfo.getHostProperties());
        this.connectionMode = TdsqlConnectionModeEnum.UNKNOWN;
        this.host = hostInfo.getHost();
        this.port = hostInfo.getPort();
        this.user = hostInfo.getUser();
        this.password = hostInfo.getPassword();
        this.hostProperties = new PropertyMap<>(hostInfo.getHostProperties());
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
        this.hostProperties = new PropertyMap<>(hostInfo.getHostProperties());
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

    public void setHeartbeatIntervalTime(long heartbeatIntervalTime) {
        this.heartbeatIntervalTime = heartbeatIntervalTime;
    }

    public long getHeartbeatIntervalTime() {
        return heartbeatIntervalTime;
    }

    private class PropertyMap<K, V> extends HashMap<K, V> {

        public PropertyMap(Map<K, V> map) {
            super(map);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof PropertyMap))
                return false;
            PropertyMap<?, ?> other = (PropertyMap<?, ?>) obj;
            if (size() != other.size())
                return false;
            for (Map.Entry<K, V> entry : entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (!other.containsKey(key) || !other.get(key).equals(value))
                    return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (Map.Entry<K, V> entry : entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                result = 31 * result + (key == null ? 0 : key.hashCode());
                result = 31 * result + (value == null ? 0 : value.hashCode());
            }
            return result;
        }
    }
}
