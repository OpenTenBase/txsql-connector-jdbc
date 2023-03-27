package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.cluster;

import java.util.Objects;

/**
 * The type Data set info.
 */
public class TdsqlDataSetInfo {

    // 数据节点ip地址
    private String ip;
    // 数据节点端口号
    private String port;
    // 权重, 0-100
    private Integer weight = 0;
    // 是否存活
    private Boolean alive = true;
    // true: 是监听节点, false: 正常节点
    private Boolean watch = false;
    // 数据同步延迟, >= 0的整数
    private Long delay = 0L;

    public TdsqlDataSetInfo(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public Boolean getAlive() {
        return alive;
    }

    public void setAlive(Boolean alive) {
        this.alive = alive;
    }

    public Boolean getWatch() {
        return watch;
    }

    public void setWatch(Boolean watch) {
        this.watch = watch;
    }

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdsqlDataSetInfo that = (TdsqlDataSetInfo) o;
        return Objects.equals(ip, that.ip) && Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    public TdsqlDataSetInfo copy() {
        TdsqlDataSetInfo res = new TdsqlDataSetInfo(this.getIp(), this.getPort());
        res.setWeight(this.getWeight());
        res.setWatch(this.getWatch());
        res.setDelay(this.getDelay());
        res.setAlive(this.getAlive());
        return res;
    }

    @Override
    public String toString() {
        return ip + ":" + port + ":" + delay;
    }
}
